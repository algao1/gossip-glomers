package main

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"log"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	skv := maelstrom.NewSeqKV(n)
	lkv := maelstrom.NewLinKV(n)
	s := newServer(n, skv, lkv)

	n.Handle("send", s.send)
	n.Handle("poll", s.poll)
	n.Handle("commit_offsets", s.commitOffsets)
	n.Handle("list_committed_offsets", s.listCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	mu         sync.Mutex
	node       *maelstrom.Node
	skv        *maelstrom.KV
	lkv        *maelstrom.KV
	partitions map[string]*partition
}

type partition struct {
	log             []logMessage
	persistedOffset int
}

type logMessage struct {
	Message int
	Offset  int
}

func newServer(node *maelstrom.Node, skv, lkv *maelstrom.KV) *server {
	s := server{
		node:       node,
		skv:        skv,
		lkv:        lkv,
		partitions: make(map[string]*partition),
	}
	go s.update()
	return &s
}

func (s *server) getOwner(key string) string {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	i := int(hash.Sum32())
	if i < 0 {
		i *= -1
	}
	return s.node.NodeIDs()[i%len(s.node.NodeIDs())]
}

func (s *server) update() {
	for range time.NewTicker(2500 * time.Millisecond).C {
		s.mu.Lock()
		for k, partition := range s.partitions {
			if partition.persistedOffset == len(partition.log) {
				continue
			}
			s.lkv.Write(context.Background(), k, partition.log)
			partition.persistedOffset = len(partition.log)
		}
		s.mu.Unlock()
	}
}

func (s *server) send(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["key"].(string)
	msgInt := int(body["msg"].(float64))

	resp := make(map[string]any)
	resp["type"] = "send_ok"

	owner := s.getOwner(key)
	if owner == s.node.ID() {
		s.mu.Lock()
		if _, ok := s.partitions[key]; !ok {
			s.partitions[key] = &partition{log: make([]logMessage, 0)}
		}

		logLen := len(s.partitions[key].log)
		s.partitions[key].log = append(s.partitions[key].log, logMessage{
			Offset:  logLen,
			Message: msgInt,
		})
		resp["offset"] = logLen
		s.mu.Unlock()
	} else {
		relayedMsg, err := s.node.SyncRPC(context.Background(), owner, body)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(relayedMsg.Body, &body); err != nil {
			return err
		}
		resp["offset"] = int(body["offset"].(float64))
	}

	return s.node.Reply(msg, resp)
}

func (s *server) poll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "poll_ok"

	rawOffsets := body["offsets"].(map[string]any)
	ret := make(map[string][][]int)
	var rmu sync.Mutex

	var wg sync.WaitGroup
	for key, v := range rawOffsets {
		wg.Add(1)
		go func(key string, v any) {
			owner := s.getOwner(key)
			var log []logMessage

			if owner == s.node.ID() {
				s.mu.Lock()
				if _, ok := s.partitions[key]; ok {
					log = s.partitions[key].log
				}
				s.mu.Unlock()
			} else {
				s.lkv.ReadInto(context.Background(), key, &log)
			}

			logT := make([][]int, 0)
			for i := len(log) - 1; i >= 0; i-- {
				if log[i].Offset < int(v.(float64)) {
					break
				}
				logT = append(logT, []int{log[i].Offset, log[i].Message})
			}
			slices.Reverse(logT)

			rmu.Lock()
			ret[key] = logT
			rmu.Unlock()
			wg.Done()
		}(key, v)
	}
	wg.Wait()

	resp["msgs"] = ret
	return s.node.Reply(msg, resp)
}

func (s *server) commitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "commit_offsets_ok"

	rawOffsets := body["offsets"].(map[string]any)

	for k, v := range rawOffsets {
		offset := int(v.(float64))
		for {
			oldOffset, err := s.skv.ReadInt(context.Background(), k)
			if err != nil {
				oldOffset = 0
			}
			if oldOffset > offset {
				break
			}

			err = s.skv.CompareAndSwap(context.Background(), k, oldOffset, offset, true)
			if err == nil {
				break
			}
		}
	}

	return s.node.Reply(msg, resp)
}

func (s *server) listCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "list_committed_offsets_ok"

	ret := make(map[string]int)

	rawKeys := body["keys"].([]any)
	for _, v := range rawKeys {
		key := v.(string)
		offset, err := s.skv.ReadInt(context.Background(), key)
		if err != nil {
			offset = 0
		}
		ret[key] = offset
	}

	resp["offsets"] = ret
	return s.node.Reply(msg, resp)
}
