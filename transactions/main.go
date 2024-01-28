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

	n.Handle("txn", s.txn)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	mu        sync.Mutex
	node      *maelstrom.Node
	registers map[int]int
}

func newServer(n *maelstrom.Node) *server {
	s := &server{
		node:      n,
		registers: make(map[int]int),
	}
	return s
}

func (s *server) txn(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "txn_ok"
	ops := body["txn"].([]any)
	ret := make([][]any, 0)

	s.mu.Lock()
	for _, op := range ops {
		opParams := op.([]any)
		opType := opParams[0].(string)
		key := int(opParams[1].(float64))

		switch opType {
		case "r":
			ret = append(ret, []any{"r", key, s.registers[key]})
		case "w":
			val := int(opParams[2].(float64))
			s.registers[key] = val
			ret = append(ret, []any{"w", key, val})
		}
	}
	s.mu.Unlock()

	body["txn"] = ret
	return s.node.Reply(msg, body)
}
