package main

import maelstrom "github.com/jepsen-io/maelstrom/demo/go"

func handlers(node *maelstrom.Node) {
	node.Handle("send", func(msg maelstrom.Message) error {
		offset := 1000
		response := &sendResponse{MessageType: "send_ok", Offset: offset}
		return node.Reply(msg, response)

	})
}
