package main

import (
	"sync"
)

type safeLogger struct {
	loggerLock sync.RWMutex
	keyStores  map[string]keyStores
	committed  map[string]int
}

type keyStores struct {
	messages []message
	offsets  int
}

type message struct {
	messageSent int
	offset      int
}

func newSafeLogger() *safeLogger {
	return &safeLogger{
		keyStores: make(map[string]keyStores),
		committed: make(map[string]int),
	}
}

func (sL *safeLogger) Add(key string, value int) int {
	sL.loggerLock.Lock()
	defer sL.loggerLock.Unlock()

	keyStore := sL.keyStores[key]
	offset := keyStore.offsets

	keyStore.messages = append(keyStore.messages, message{
		offset:      offset,
		messageSent: value,
	})
	keyStore.offsets++
	sL.keyStores[key] = keyStore

	return offset
}

func (sL *safeLogger) Poll(offsets map[string]int) map[string][][]int {
	sL.loggerLock.RLock()
	defer sL.loggerLock.RUnlock()

	result := make(map[string][][]int, len(offsets))
	for key, requestedOffset := range offsets {
		result[key] = make([][]int, 0)
		for _, storedMessage := range sL.keyStores[key].messages {
			if storedMessage.offset >= requestedOffset {
				result[key] = append(result[key], []int{storedMessage.offset, storedMessage.messageSent})
			}
		}
	}

	return result
}

func (sL *safeLogger) CommitOffsets(offsets map[string]int) {
	sL.loggerLock.Lock()
	defer sL.loggerLock.Unlock()

	for key, offset := range offsets {
		sL.committed[key] = offset
	}
}

func (sL *safeLogger) ListCommittedOffsets(keys []string) map[string]int {
	sL.loggerLock.RLock()
	defer sL.loggerLock.RUnlock()

	result := make(map[string]int, len(keys))
	for _, key := range keys {
		if offset, exists := sL.committed[key]; exists {
			result[key] = offset
		}
	}

	return result
}
