package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
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
	mu       sync.Mutex
	node     *maelstrom.Node
	messages []int
}

func newServer(n *maelstrom.Node) *server {
	return &server{
		node:     n,
		messages: make([]int, 0),
	}
}

func (s *server) broadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "broadcast_ok"

	s.mu.Lock()
	s.messages = append(s.messages, int(body["message"].(float64)))
	s.mu.Unlock()

	return s.node.Reply(msg, resp)
}

func (s *server) read(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "read_ok"

	s.mu.Lock()
	resp["messages"] = s.messages
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

	return s.node.Reply(msg, resp)
}
