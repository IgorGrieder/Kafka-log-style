package main

import (
	"reflect"
	"sync"
	"testing"
)

func TestAddAllocatesOffsetsPerLog(t *testing.T) {
	store := newKafkaStore()

	if offset := store.Add("orders", 10); offset != 0 {
		t.Fatalf("first orders offset = %d, want 0", offset)
	}
	if offset := store.Add("orders", 20); offset != 1 {
		t.Fatalf("second orders offset = %d, want 1", offset)
	}
	if offset := store.Add("payments", 30); offset != 0 {
		t.Fatalf("first payments offset = %d, want 0", offset)
	}
}

func TestPollReturnsOffsetValuePairsFromRequestedOffset(t *testing.T) {
	store := newKafkaStore()
	store.Add("orders", 10)
	store.Add("orders", 20)
	store.Add("orders", 30)

	got := store.Poll(map[string]int{"orders": 1, "missing": 0})
	want := map[string][][]int{"orders": {{1, 20}, {2, 30}}, "missing": {}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Poll() = %#v, want %#v", got, want)
	}
}

func TestCommittedOffsetsStoreLatestValue(t *testing.T) {
	store := newKafkaStore()
	store.CommitOffsets(map[string]int{"orders": 4, "payments": 0})
	store.CommitOffsets(map[string]int{"orders": 2})

	got := store.ListCommittedOffsets([]string{"orders", "payments", "missing"})
	want := map[string]int{"orders": 2, "payments": 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListCommittedOffsets() = %#v, want %#v", got, want)
	}
}

func TestConcurrentAddsAllocateUniqueOffsets(t *testing.T) {
	store := newKafkaStore()
	const count = 100

	offsets := make(chan int, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(value int) {
			defer wg.Done()
			offsets <- store.Add("orders", value)
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
