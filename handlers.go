package main

import (
	"context"
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func handlers(node *maelstrom.Node) {
	store := newKafkaStore(maelstrom.NewLinKV(node))

	node.Handle("send", func(msg maelstrom.Message) error {
		var body sendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offset, err := store.Add(context.Background(), body.Key, body.Msg)
		if err != nil {
			return err
		}

		response := &sendResponse{MessageType: "send_ok", Offset: offset}
		return node.Reply(msg, response)
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body pollRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages, err := store.Poll(context.Background(), body.Offsets)
		if err != nil {
			return err
		}
		response := &pollResponse{
			MessageType: "poll_ok",
			Messages:    messages,
		}
		return node.Reply(msg, response)
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body commitOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if err := store.CommitOffsets(context.Background(), body.Offsets); err != nil {
			return err
		}
		return node.Reply(msg, &commitOffsetsResponse{MessageType: "commit_offsets_ok"})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body listCommittedOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets, err := store.ListCommittedOffsets(context.Background(), body.Keys)
		if err != nil {
			return err
		}
		response := &listCommittedOffsetsResponse{
			MessageType: "list_committed_offsets_ok",
			Offsets:     offsets,
		}
		return node.Reply(msg, response)
	})
}
