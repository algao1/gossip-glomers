package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/maps"
)

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)

	n.Handle("broadcast", s.broadcast)
	n.Handle("read", s.read)
	n.Handle("topology", s.topology)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	mu          sync.Mutex
	node        *maelstrom.Node
	neighbours  []string
	messages    map[int]struct{}
	toBroadcast chan int
}

func newServer(n *maelstrom.Node) *server {
	s := server{
		node:        n,
		neighbours:  make([]string, 0),
		messages:    make(map[int]struct{}),
		toBroadcast: make(chan int, 100),
	}
	go s.gossip()
	return &s
}

func (s *server) gossip() {
	for m := range s.toBroadcast {
		s.mu.Lock()
		for _, n := range s.neighbours {
			msg := map[string]any{
				"type":    "broadcast",
				"message": float64(m),
			}
			s.node.RPC(n, msg, nil)
		}
		s.mu.Unlock()
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
		s.messages[m] = struct{}{}
		s.toBroadcast <- m
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
	s.neighbours = make([]string, len(topoRaw))
	for i, n := range topoRaw {
		s.neighbours[i] = n.(string)
	}
	s.mu.Unlock()

	return s.node.Reply(msg, resp)
}
