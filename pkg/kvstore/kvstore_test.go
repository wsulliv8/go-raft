package kvstore

import (
	"testing"
	"sync"
	"fmt"
)

func TestNewKVStore(t *testing.T) {
	kv := NewKVStore()
	if kv == nil {
		t.Fatal("NewKVStore() returned nil")
	}

	if len(kv.store) != 0 {
		t.Fatal("NewKVStore() returned non-empty store")
	}
}

func TestSetAndGet(t *testing.T) {
	kv := NewKVStore()

	key := "test-key"
	value := "test-value"

	kv.Set(key, value)

	val, ok := kv.Get(key)
	if !ok {
		t.Fatal("Get() failed")
	}

	if val != value {
		t.Fatalf("Set() and Get() failed: got %q, want %q", val, value)
	}

	_, ok = kv.Get("non-existent-key")
	if ok {
		t.Fatal("Get() returned true for non-existent key")
	}
}

func TestSetAndDelete(t *testing.T) {
	kv := NewKVStore()

	key := "test-key"
	value := "test-value"

	kv.Set(key, value)

	_, ok := kv.Get(key)
	if !ok {
		t.Fatal("Key not found after Set()")
	}

	kv.Delete(key)

	_, ok = kv.Get(key)
	if ok {
		t.Fatal("Delete() failed to delete key")
	}
}

func TestConcurrentSetAndGet(t *testing.T) {
	kv := NewKVStore()
	const numGoroutines = 100

	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			kv.Set(key, value)
		}(i)
	}
	wg.Wait()

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			val, ok := kv.Get(key)
			if !ok {
				t.Errorf("Key not found after Set()")
			}
			if val != value {
				t.Errorf("Set() and Get() failed: got %q, want %q", val, value)
			}
		}(i)
	}
	wg.Wait()
}