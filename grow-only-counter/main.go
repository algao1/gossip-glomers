package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func errPrint(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format, args...)
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	server := &server{
		node: n,
		kv:   kv,
	}
	n.Handle("add", server.add)
	n.Handle("read", server.read)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func (s *server) add(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "add_ok"
	delta := int(body["delta"].(float64))

	go func(delta int) {
		for {
			v, err := s.kv.ReadInt(context.Background(), "val")
			if err != nil {
				v = 0
			}

			errPrint("%s: %d to %d\n", s.node.ID(), v, v+delta)
			err = s.kv.CompareAndSwap(context.Background(), "val", v, v+delta, true)
			if err == nil {
				return
			}
		}
	}(delta)

	return s.node.Reply(msg, resp)
}

func (s *server) read(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "read_ok"

	v, _ := s.kv.ReadInt(context.Background(), "val")
	resp["value"] = v

	return s.node.Reply(msg, resp)
}
