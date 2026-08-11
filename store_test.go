package main

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type memoryKV struct {
	mu     sync.Mutex
	values map[string]any
}

func newMemoryKV() *memoryKV {
	return &memoryKV{values: make(map[string]any)}
}

func (kv *memoryKV) ReadInto(_ context.Context, key string, destination any) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exists := kv.values[key]
	if !exists {
		return maelstrom.NewRPCError(maelstrom.KeyDoesNotExist, "missing key")
	}
	encoded, _ := json.Marshal(value)
	return json.Unmarshal(encoded, destination)
}

func (kv *memoryKV) Write(_ context.Context, key string, value any) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.values[key] = value
	return nil
}

func (kv *memoryKV) CompareAndSwap(_ context.Context, key string, from, to any, create bool) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	current, exists := kv.values[key]
	if !exists && !create {
		return maelstrom.NewRPCError(maelstrom.KeyDoesNotExist, "missing key")
	}
	if exists && !reflect.DeepEqual(current, from) || !exists && from != nil {
		return maelstrom.NewRPCError(maelstrom.PreconditionFailed, "value changed")
	}
	kv.values[key] = to
	return nil
}

func TestAddAllocatesOffsetsAcrossStoreInstances(t *testing.T) {
	kv := newMemoryKV()
	firstNode := newKafkaStore(kv)
	secondNode := newKafkaStore(kv)

	firstOffset, err := firstNode.Add(context.Background(), "orders", 10)
	if err != nil {
		t.Fatal(err)
	}
	secondOffset, err := secondNode.Add(context.Background(), "orders", 20)
	if err != nil {
		t.Fatal(err)
	}

	if firstOffset != 0 || secondOffset != 1 {
		t.Fatalf("offsets = (%d, %d), want (0, 1)", firstOffset, secondOffset)
	}
}

func TestPollReadsSharedMessagesAtRequestedOffset(t *testing.T) {
	kv := newMemoryKV()
	writer := newKafkaStore(kv)
	reader := newKafkaStore(kv)
	for _, value := range []int{10, 20, 30} {
		if _, err := writer.Add(context.Background(), "orders", value); err != nil {
			t.Fatal(err)
		}
	}

	got, err := reader.Poll(context.Background(), map[string]int{"orders": 1, "missing": 0})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][][]int{"orders": {{1, 20}, {2, 30}}, "missing": {}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Poll() = %#v, want %#v", got, want)
	}
}

func TestCommittedOffsetsAreShared(t *testing.T) {
	kv := newMemoryKV()
	writer := newKafkaStore(kv)
	reader := newKafkaStore(kv)

	if err := writer.CommitOffsets(context.Background(), map[string]int{"orders": 4, "payments": 0}); err != nil {
		t.Fatal(err)
	}
	got, err := reader.ListCommittedOffsets(context.Background(), []string{"orders", "payments", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"orders": 4, "payments": 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListCommittedOffsets() = %#v, want %#v", got, want)
	}
}

func TestConcurrentAddsAllocateUniqueOffsets(t *testing.T) {
	kv := newMemoryKV()
	stores := []*kafkaStore{newKafkaStore(kv), newKafkaStore(kv)}
	const count = 100

	offsets := make(chan int, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(value int) {
			defer wg.Done()
			offset, err := stores[value%len(stores)].Add(context.Background(), "orders", value)
			if err != nil {
				t.Errorf("Add() error = %v", err)
				return
			}
			offsets <- offset
		}(i)
	}
	wg.Wait()
	close(offsets)

	seen := make(map[int]bool, count)
	for offset := range offsets {
		if seen[offset] {
			t.Fatalf("duplicate offset %d", offset)
		}
		seen[offset] = true
	}
	if len(seen) != count {
		t.Fatalf("allocated %d offsets, want %d", len(seen), count)
	}
}
