package main

import "sync"

type kafkaStore struct {
	mu        sync.RWMutex
	logs      map[string][][]int
	committed map[string]int
}

func newKafkaStore() *kafkaStore {
	return &kafkaStore{
		logs:      make(map[string][][]int),
		committed: make(map[string]int),
	}
}

func (s *kafkaStore) Add(key string, value int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := len(s.logs[key])
	s.logs[key] = append(s.logs[key], []int{offset, value})
	return offset
}

func (s *kafkaStore) Poll(offsets map[string]int) map[string][][]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string][][]int, len(offsets))
	for key, requestedOffset := range offsets {
		messages := s.logs[key]
		start := requestedOffset
		if start < 0 {
			start = 0
		}
		if start > len(messages) {
			start = len(messages)
		}

		result[key] = append([][]int(nil), messages[start:]...)
		if result[key] == nil {
			result[key] = make([][]int, 0)
		}
	}
	return result
}

func (s *kafkaStore) CommitOffsets(offsets map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, offset := range offsets {
		s.committed[key] = offset
	}
}

func (s *kafkaStore) ListCommittedOffsets(keys []string) map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]int, len(keys))
	for _, key := range keys {
		if offset, exists := s.committed[key]; exists {
			result[key] = offset
		}
	}
	return result
}
