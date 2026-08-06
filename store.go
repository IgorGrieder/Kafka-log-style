package main

import "sync"

type safeLogger struct {
	loggerLock sync.RWMutex
	keyStores  map[string]keyStores
}

type keyStores struct {
	keyLock  sync.RWMutex
	messages map[string]int
	offsets  int
}
