package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/maps"
)

func print(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format, args...)
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("broadcast", s.broadcast)
	n.Handle("batch_broadcast", s.batchBroadcast)
	n.Handle("read", s.read)
	n.Handle("topology", s.topology)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	mu         sync.Mutex
	node       *maelstrom.Node
	neighbours map[string]chan int
	messages   map[int]struct{}
}

func newServer(n *maelstrom.Node) *server {
	s := server{
		node:       n,
		neighbours: make(map[string]chan int),
		messages:   make(map[int]struct{}),
	}
	return &s
}

func (s *server) gossip() {
	for n, nChan := range s.neighbours {
		go func(n string, nChan chan int) {
			buffer := make([]int, 0)
			ticker := time.NewTicker(time.Duration(150) * time.Millisecond)

			for {
				select {
				case <-ticker.C:
					l := len(buffer)
					batch := buffer[:l]
					buffer = buffer[l:]

					if len(batch) == 0 {
						continue
					}

					msg := map[string]any{
						"type":            "batch_broadcast",
						"batched_message": batch,
					}

					closer := make(chan struct{})
					go func(batch []int) {
						for {
							select {
							case <-closer:
								return
							case <-time.After(250 * time.Millisecond):
								for _, m := range batch {
									nChan <- m
								}
								return
							}
						}
					}(batch)

					s.node.RPC(n, msg, func(respMsg maelstrom.Message) error {
						if respMsg.Type() == "batch_broadcast_ok" {
							close(closer)
						}
						return nil
					})
				case m := <-nChan:
					buffer = append(buffer, m)
				}
			}
		}(n, nChan)
	}
}

func (s *server) broadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "broadcast_ok"
	m := int(body["message"].(float64))

	s.mu.Lock()
	if _, ok := s.messages[m]; !ok {
		for _, nChan := range s.neighbours {
			nChan <- m
		}
	}
	s.messages[m] = struct{}{}
	s.mu.Unlock()

	if _, ok := body["msg_id"]; ok {
		return s.node.Reply(msg, resp)
	}
	return nil
}

func (s *server) batchBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "batch_broadcast_ok"
	batch := body["batched_message"].([]any)

	s.mu.Lock()
	for _, bm := range batch {
		m := int(bm.(float64))
		if _, ok := s.messages[m]; !ok {
			for _, nChan := range s.neighbours {
				nChan <- m
			}
		}
		s.messages[m] = struct{}{}
	}
	s.mu.Unlock()

	if _, ok := body["msg_id"]; ok {
		return s.node.Reply(msg, resp)
	}
	return nil
}

func (s *server) read(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "read_ok"

	s.mu.Lock()
	resp["messages"] = maps.Keys[map[int]struct{}](s.messages)
	s.mu.Unlock()

	return s.node.Reply(msg, resp)
}

func (s *server) topology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "topology_ok"

	s.mu.Lock()
	topoRaw := body["topology"].(map[string]any)[s.node.ID()].([]any)
	s.neighbours = make(map[string]chan int)
	for _, n := range topoRaw {
		s.neighbours[n.(string)] = make(chan int, 1000)
	}
	s.gossip()
	s.mu.Unlock()

	return s.node.Reply(msg, resp)
}
