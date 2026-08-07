package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func handlers(node *maelstrom.Node) {
	store := newSafeLogger()

	node.Handle("send", func(msg maelstrom.Message) error {
		var body sendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offset := store.Add(body.Key, body.Msg)

		response := &sendResponse{MessageType: "send_ok", Offset: offset}
		return node.Reply(msg, response)
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body pollRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := &pollResponse{
			MessageType: "poll_ok",
			Messages:    store.Poll(body.Offsets),
		}
		return node.Reply(msg, response)
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body commitOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		store.CommitOffsets(body.Offsets)
		return node.Reply(msg, &commitOffsetsResponse{MessageType: "commit_offsets_ok"})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body listCommittedOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := &listCommittedOffsetsResponse{
			MessageType: "list_committed_offsets_ok",
			Offsets:     store.ListCommittedOffsets(body.Keys),
		}
		return node.Reply(msg, response)
	})
}
