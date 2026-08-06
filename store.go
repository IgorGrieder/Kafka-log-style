package main

import "sync"

type safeLogger struct {
	loggerLock sync.RWMutex
}

type keyStores struct {
	keyLock  sync.RWMutex
	messages map[string]int
	offsets  map[string]int
}
