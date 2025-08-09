package raft

import (
	"sync"
)

type LogEntry struct {
	Term int
	Command string
}

type Log struct {
	mu sync.RWMutex
	entries []LogEntry
}