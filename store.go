package main

import (
	"context"
	"errors"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	logKeyPrefix       = "log/"
	committedKeyPrefix = "committed/"
)

type keyValueStore interface {
	ReadInto(context.Context, string, any) error
	Write(context.Context, string, any) error
	CompareAndSwap(context.Context, string, any, any, bool) error
}

type kafkaStore struct {
	kv keyValueStore
}

func newKafkaStore(kv keyValueStore) *kafkaStore {
	return &kafkaStore{kv: kv}
}

func (s *kafkaStore) Add(ctx context.Context, key string, value int) (int, error) {
	storageKey := logKeyPrefix + key

	for {
		var messages [][]int
		err := s.kv.ReadInto(ctx, storageKey, &messages)

		var previous any = messages
		if isRPCError(err, maelstrom.KeyDoesNotExist) {
			messages = make([][]int, 0)
			previous = nil
		} else if err != nil {
			return 0, fmt.Errorf("read log %q: %w", key, err)
		}

		offset := len(messages)
		updated := append(append([][]int(nil), messages...), []int{offset, value})
		err = s.kv.CompareAndSwap(ctx, storageKey, previous, updated, true)
		if isRPCError(err, maelstrom.PreconditionFailed) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("append to log %q: %w", key, err)
		}

		return offset, nil
	}
}

func (s *kafkaStore) Poll(ctx context.Context, offsets map[string]int) (map[string][][]int, error) {
	result := make(map[string][][]int, len(offsets))

	for key, requestedOffset := range offsets {
		var messages [][]int
		err := s.kv.ReadInto(ctx, logKeyPrefix+key, &messages)
		if isRPCError(err, maelstrom.KeyDoesNotExist) {
			result[key] = make([][]int, 0)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read log %q: %w", key, err)
		}

		result[key] = make([][]int, 0)
		for _, message := range messages {
			if message[0] >= requestedOffset {
				result[key] = append(result[key], message)
			}
		}
	}

	return result, nil
}

func (s *kafkaStore) CommitOffsets(ctx context.Context, offsets map[string]int) error {
	for key, offset := range offsets {
		if err := s.kv.Write(ctx, committedKeyPrefix+key, offset); err != nil {
			return fmt.Errorf("commit offset for %q: %w", key, err)
		}
	}
	return nil
}

func (s *kafkaStore) ListCommittedOffsets(ctx context.Context, keys []string) (map[string]int, error) {
	result := make(map[string]int, len(keys))

	for _, key := range keys {
		var offset int
		err := s.kv.ReadInto(ctx, committedKeyPrefix+key, &offset)
		if isRPCError(err, maelstrom.KeyDoesNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read committed offset for %q: %w", key, err)
		}
		result[key] = offset
	}

	return result, nil
}

func isRPCError(err error, code int) bool {
	var rpcErr *maelstrom.RPCError
	return errors.As(err, &rpcErr) && rpcErr.Code == code
}
