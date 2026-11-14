package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"redis-server/pubsub"
	"redis-server/resp"
	"redis-server/store"
)

func HandleConnection(c net.Conn) {
	fmt.Printf("New connection: %s\n", c.RemoteAddr().String())

	activeSubs := make(map[string]chan string)

	defer func() {
		for topic, ch := range activeSubs {
			pubsub.Unsubscribe(topic, ch)
		}
		c.Close()
		fmt.Printf("Client disconnected: %s\n", c.RemoteAddr().String())
	}()

	reader := bufio.NewReader(c)

	for {
		val, err := resp.ParseRESP(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error trying to read:", err)
			}
			break
		}

		if val.Typ != "array" || len(val.Array) == 0 {
			continue
		}

		command := strings.ToUpper(val.Array[0].Str)

		switch command {

		case "PING":
			c.Write([]byte("+PONG\r\n"))

		case "SET":
			if len(val.Array) >= 3 {
				store.Set(val.Array[1].Str, val.Array[2].Str)
				c.Write([]byte("+OK\r\n"))
			}

		case "GET":
			if len(val.Array) >= 2 {
				strVal, exists, err := store.GetString(val.Array[1].Str)

				if !exists {
					c.Write([]byte("$-1\r\n"))
				} else if err != nil {
					c.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
				} else {
					c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(strVal), strVal)))
				}
			}
		case "DEL":
			if len(val.Array) < 2 {
				c.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
				break
			}

			numDeletions := 0

			for i := 1; i < len(val.Array); i++ {
				keyText := val.Array[i].Str

				if store.Delete(keyText) {
					numDeletions++
				}
			}

			c.Write([]byte(fmt.Sprintf(":%d\r\n", numDeletions)))

		case "LRANGE":
			if len(val.Array) < 4 {
				c.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n"))
				break
			}

			keyText := val.Array[1].Str
			start, _ := strconv.Atoi(val.Array[2].Str)
			stop, _ := strconv.Atoi(val.Array[3].Str)

			list, exists, err := store.GetList(keyText)

			if !exists {
				c.Write([]byte("*0\r\n"))
				break
			}
			if err != nil {
				c.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
				break
			}

			if start < 0 {
				start = len(list) + start
			}
			if stop < 0 {
				stop = len(list) + stop
			}
			if start < 0 {
				start = 0
			}
			if stop >= len(list) {
				stop = len(list) - 1
			}

			if start > stop || start >= len(list) {
				c.Write([]byte("*0\r\n"))
				break
			}

			finalList := list[start : stop+1]
			respMsg := fmt.Sprintf("*%d\r\n", len(finalList))

			for _, word := range finalList {
				respMsg += fmt.Sprintf("$%d\r\n%s\r\n", len(word), word)
			}
			c.Write([]byte(respMsg))

		case "LPUSH":
			if len(val.Array) >= 3 {
				size, err := store.LPush(val.Array[1].Str, val.Array[2].Str)
				if err != nil {
					c.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
				} else {
					c.Write([]byte(fmt.Sprintf(":%d\r\n", size)))
				}
			}

		case "PUBLISH":
			if len(val.Array) >= 3 {
				topic := val.Array[1].Str
				msg := val.Array[2].Str

				recebedores := pubsub.Publish(topic, msg)
				c.Write([]byte(fmt.Sprintf(":%d\r\n", recebedores)))
			}

		case "SUBSCRIBE":
			if len(val.Array) >= 2 {
				topic := val.Array[1].Str

				myChannel := pubsub.Subscribe(topic)
				activeSubs[topic] = myChannel

				responseSub := fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(topic), topic)
				c.Write([]byte(responseSub))

				go func(t string, ch chan string) {
					for msg := range ch {
						respMsg := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
							len(t), t, len(msg), msg)
						c.Write([]byte(respMsg))
					}
				}(topic, myChannel)
			}

		case "UNSUBSCRIBE":
			if len(val.Array) >= 2 {
				topic := val.Array[1].Str

				channel, exists := activeSubs[topic]
				if exists {
					pubsub.Unsubscribe(topic, channel)
					delete(activeSubs, topic)

					resp := fmt.Sprintf("*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:0\r\n", len(topic), topic)
					c.Write([]byte(resp))
				} else {
					c.Write([]byte("-ERR not subscribed to this topic\r\n"))
				}
			}

		case "KEYS":
			if len(val.Array) < 2 {
				c.Write([]byte("-ERR wrong number of arguments for 'keys' command\r\n"))
				break
			}

			keys := store.Keys()

			respMsg := fmt.Sprintf("*%d\r\n", len(keys))
			for _, k := range keys {
				respMsg += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
			}

			c.Write([]byte(respMsg))

		case "COMMAND":
			c.Write([]byte("*0\r\n"))

		case "CONFIG":
			c.Write([]byte("*0\r\n"))

		default:
			c.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command)))
		}
	}
}
