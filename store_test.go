package main

import (
	"reflect"
	"testing"
)

func TestAddAllocatesOffsetsPerKey(t *testing.T) {
	store := newSafeLogger()

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

func TestPollReturnsMessagesAtOrAfterRequestedOffset(t *testing.T) {
	store := newSafeLogger()
	store.Add("orders", 10)
	store.Add("orders", 20)
	store.Add("orders", 30)

	got := store.Poll(map[string]int{"orders": 1, "missing": 0})
	want := map[string][][]int{
		"orders":  {{1, 20}, {2, 30}},
		"missing": {},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Poll() = %#v, want %#v", got, want)
	}
}

func TestCommittedOffsetsStoreLatestValue(t *testing.T) {
	store := newSafeLogger()

	store.CommitOffsets(map[string]int{"orders": 4, "payments": 0})
	store.CommitOffsets(map[string]int{"orders": 2})

	got := store.ListCommittedOffsets([]string{"orders", "payments", "missing"})
	want := map[string]int{"orders": 2, "payments": 0}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListCommittedOffsets() = %#v, want %#v", got, want)
	}
}
