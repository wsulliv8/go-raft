package raft

import (
	"sync"
)

type LogEntry struct {
	Index int
	Term int
	Command []byte
}

type Log struct {
	mu sync.RWMutex
	entries []LogEntry
}