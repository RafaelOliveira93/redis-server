package store

import (
	"testing"
	"time"
)

func TestSetAndGetString(t *testing.T) {
	key := "test:name"
	expectedVal := "Rafael"

	Set(key, expectedVal)

	val, exists, err := GetString(key)

	if !exists {
		t.Fatalf("Error: Expected key '%s' to exist in memory", key)
	}
	if err != nil {
		t.Fatalf("Unexpected error while fetching string: %v", err)
	}
	if val != expectedVal {
		t.Errorf("Expected value '%s', but got '%s'", expectedVal, val)
	}
}

func TestKeysCommand(t *testing.T) {
	Set("key_a", "1")
	Set("key_b", "2")

	keys := Keys()

	foundA := false
	foundB := false

	for _, k := range keys {
		if k == "key_a" {
			foundA = true
		}
		if k == "key_b" {
			foundB = true
		}
	}

	if !foundA || !foundB {
		t.Errorf("KEYS command failed. Returned keys: %v", keys)
	}
}

func TestKeyExpiration(t *testing.T) {
	key := "key:temporary"
	Set(key, "volatile_data")

	mu.Lock()
	item := memory[key]
	item.ExpiresIn = time.Now().Add(-2 * time.Second)
	memory[key] = item
	mu.Unlock()

	_, exists, err := GetString(key)

	if exists {
		t.Errorf("Expiration failed: The key should have been deleted automatically")
	}
	if err != nil {
		t.Errorf("Did not expect a type error, but got: %v", err)
	}
}
