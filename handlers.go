package main

import (
	"context"
	"encoding/json"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const forwardTimeout = 5 * time.Second

func handlers(node *maelstrom.Node) {
	store := newKafkaStore()

	node.Handle("send", func(msg maelstrom.Message) error {
		var body sendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if shouldForward(node) {
			return forwardRequest(node, msg, body)
		}

		offset := store.Add(body.Key, body.Msg)
		return node.Reply(msg, &sendResponse{MessageType: "send_ok", Offset: offset})
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body pollRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if shouldForward(node) {
			return forwardRequest(node, msg, body)
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
		if shouldForward(node) {
			return forwardRequest(node, msg, body)
		}

		store.CommitOffsets(body.Offsets)
		return node.Reply(msg, &commitOffsetsResponse{MessageType: "commit_offsets_ok"})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body listCommittedOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if shouldForward(node) {
			return forwardRequest(node, msg, body)
		}

		response := &listCommittedOffsetsResponse{
			MessageType: "list_committed_offsets_ok",
			Offsets:     store.ListCommittedOffsets(body.Keys),
		}
		return node.Reply(msg, response)
	})
}

func shouldForward(node *maelstrom.Node) bool {
	return node.ID() != ownerNode(node)
}

func ownerNode(node *maelstrom.Node) string {
	nodeIDs := node.NodeIDs()
	if len(nodeIDs) == 0 {
		return node.ID()
	}

	owner := nodeIDs[0]
	for _, nodeID := range nodeIDs[1:] {
		if nodeID < owner {
			owner = nodeID
		}
	}
	return owner
}

func forwardRequest(node *maelstrom.Node, original maelstrom.Message, body any) error {
	ctx, cancel := context.WithTimeout(context.Background(), forwardTimeout)
	defer cancel()

	response, err := node.SyncRPC(ctx, ownerNode(node), body)
	if err != nil {
		return err
	}
	return node.Reply(original, json.RawMessage(response.Body))
}
