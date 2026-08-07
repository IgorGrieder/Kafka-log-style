package main

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func handlers(node *maelstrom.Node) {
	store := safeLogger{keyStores: make(map[string]keyStores, 0), loggerLock: sync.RWMutex{}}
	node.Handle("send", func(msg maelstrom.Message) error {
		var body sendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offset, err := store.Add(body.Key, body.Msg)
		if err != nil {
			return err
		}

		response := &sendResponse{MessageType: "send_ok", Offset: offset}
		return node.Reply(msg, response)

	})
}
