package pubsub

import (
	"testing"
	"time"
)

func TestSingleSubscriber(t *testing.T) {
	topic := "room_1"
	msg := "Hello World"

	// 1. Subscribe to the topic
	ch := Subscribe(topic)
	defer Unsubscribe(topic, ch) // Cleans up the memory after the test finishes

	// 2. Publish a message in a separate Goroutine
	go Publish(topic, msg)

	// 3. Wait for the message with a 1-second timeout
	select {
	case receivedMsg := <-ch:
		if receivedMsg != msg {
			t.Errorf("Expected message '%s', but got '%s'", msg, receivedMsg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout: Did not receive the message in time")
	}
}

func TestMultipleSubscribers(t *testing.T) {
	topic := "room_broadcast"
	msg := "System Update!"

	ch1 := Subscribe(topic)
	ch2 := Subscribe(topic)
	defer Unsubscribe(topic, ch1)
	defer Unsubscribe(topic, ch2)

	go Publish(topic, msg)

	select {
	case r1 := <-ch1:
		if r1 != msg {
			t.Errorf("Subscriber 1 expected '%s', got '%s'", msg, r1)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout: Subscriber 1 did not receive the message")
	}

	select {
	case r2 := <-ch2:
		if r2 != msg {
			t.Errorf("Subscriber 2 expected '%s', got '%s'", msg, r2)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout: Subscriber 2 did not receive the message")
	}
}

func TestUnsubscribe(t *testing.T) {
	topic := "room_closed"

	ch := Subscribe(topic)

	Unsubscribe(topic, ch)

	go Publish(topic, "This should not be received")

	select {
	case msg, ok := <-ch:
		if ok {
			t.Fatalf("Unsubscribe failed: received unexpected message: %s", msg)
		}
	case <-time.After(100 * time.Millisecond):
	}
}
