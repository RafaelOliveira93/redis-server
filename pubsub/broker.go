package pubsub

import (
	"fmt"
	"sync"
)

var (
	subs = make(map[string][]chan string)
	mu   sync.RWMutex
)

func Publish(topic string, message string) int {
	mu.RLock()
	subscribers := subs[topic]
	mu.RUnlock()

	counter := 0

	for _, channel := range subscribers {
		select {
		case channel <- message:
			counter++
		default:
			fmt.Println("Client channel has a full buffer, discarting message in topic:", topic)
		}
	}

	return counter
}

func Subscribe(topic string) chan string {
	channel := make(chan string, 100)

	mu.Lock()
	subs[topic] = append(subs[topic], channel)
	mu.Unlock()

	return channel
}

func Unsubscribe(topic string, targetChannel chan string) {
	mu.Lock()
	defer mu.Unlock()

	subscribers := subs[topic]

	for i, ch := range subscribers {
		if ch == targetChannel {
			subs[topic] = append(subscribers[:i], subscribers[i+1:]...)
			break
		}
	}

	close(targetChannel)
	fmt.Printf("Channel removed and closed on topic '%s'\n", topic)
}
