package main

import (
	"fmt"
	"strconv"
	"sync"
)

type safeLogger struct {
	loggerLock sync.RWMutex
	keyStores  map[string]keyStores
}

type keyStores struct {
	messages []message
	offsets  int
}

type message struct {
	messageSent int
	offset      int
}

func (sL *safeLogger) Add(key string, value int) (int, error) {
	sL.loggerLock.Lock()
	defer sL.loggerLock.Unlock()

	keyStore, ok := sL.keyStores[key]
	slicedKey := key[1:]

	offsettBase, err := strconv.Atoi(slicedKey)
	if err != nil {
		fmt.Println("Conversion error:", err)
		return 0, err
	}

	// If map is empty
	if !ok {
		newStore := keyStores{messages: []message{message{offset: offsettBase * 1000, messageSent: value}}, offsets: offsettBase * 1000}
		sL.keyStores[key] = newStore
		return 0, nil
	}

	offset := keyStore.offsets + 1
	keyStore.messages = append(keyStore.messages, message{offset: offset + 1, messageSent: value})
	keyStore.offsets = offset

	return offset, nil
}
