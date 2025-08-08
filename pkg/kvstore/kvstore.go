package kvstore

import (
	"sync"
)

type KVStore struct {
	mu sync.RWMutex
	store map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{store: make(map[string]string)}
}

func (s *KVStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[key] = value
}

func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if value, ok := s.store[key]; ok {
		return value, true
	}
	return "", false
}

func (s *KVStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, key)
}