package store

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type MemoryItem struct {
	Type      string
	Value     any
	ExpiresIn time.Time
}

var (
	memory = make(map[string]MemoryItem)
	mu     sync.RWMutex
)

func GetString(key string) (string, bool, error) {
	mu.RLock()
	item, exists := memory[key]
	mu.RUnlock()

	if !exists || (!item.ExpiresIn.IsZero() && time.Now().After(item.ExpiresIn)) {
		if exists {
			Delete(key)
		}
		return "", false, nil
	}

	if item.Type != "string" {
		return "", true, errors.New("wrong type")
	}

	return item.Value.(string), true, nil
}

func GetList(key string) ([]string, bool, error) {
	mu.RLock()
	item, exists := memory[key]
	mu.RUnlock()

	if !exists || (!item.ExpiresIn.IsZero() && time.Now().After(item.ExpiresIn)) {
		if exists {
			Delete(key)
		}
		return nil, false, nil
	}

	if item.Type != "list" {
		return nil, true, errors.New("wrong type")
	}

	return item.Value.([]string), true, nil
}

func Set(key string, value string) {
	mu.Lock()
	defer mu.Unlock()

	memory[key] = MemoryItem{
		Type:  "string",
		Value: value,
	}

	AppendLog(fmt.Sprintf("SET %s %s\n", key, value))
}

func LPush(key string, value string) (int, error) {
	mu.Lock()
	defer mu.Unlock()

	item, exists := memory[key]

	if exists && item.Type != "list" {
		return 0, errors.New("wrong type")
	}

	var list []string
	if exists {
		list = item.Value.([]string)
	}

	list = append([]string{value}, list...)

	memory[key] = MemoryItem{
		Type:      "list",
		Value:     list,
		ExpiresIn: item.ExpiresIn,
	}

	AppendLog(fmt.Sprintf("LPUSH %s %s\n", key, value))

	return len(list), nil
}

func Delete(key string) bool {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := memory[key]; exists {
		delete(memory, key)
		AppendLog(fmt.Sprintf("DEL %s\n", key))
		return true
	}
	return false
}

func Keys() []string {
	mu.RLock()
	defer mu.RUnlock()

	var keys []string
	now := time.Now()

	for key, item := range memory {
		if !item.ExpiresIn.IsZero() && now.After(item.ExpiresIn) {
			continue
		}
		keys = append(keys, key)
	}

	return keys
}
