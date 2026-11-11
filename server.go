package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RESPValue struct {
	Typ   string
	Str   string
	Num   int
	Array []RESPValue
}

type MemoryItem struct {
	Value     string
	ExpiresIn time.Time
}

var (
	memory  = make(map[string]MemoryItem) // RAM
	aofFile *os.File                      // DISK
	mu      sync.RWMutex

	pubsub   = make(map[string][]chan string)
	pubsubMu sync.RWMutex
)

func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Provide a port number!")
		return
	}

	loadAOF() // tries to read the database.aof

	var err error
	aofFile, err = os.OpenFile("database.aof", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Error while opening AOF: ", err)
	}

	defer aofFile.Close()

	go backgroundSync()

	PORT := ":" + arguments[1]
	l, err := net.Listen("tcp4", PORT)

	if err != nil {
		log.Fatal(err)
	}

	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Printf("Error while accepting new connection: %v\n", err)
			continue
		}

		go handleConnection(c)
	}
}

func handleConnection(c net.Conn) {
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())
	defer c.Close()

	reader := bufio.NewReader(c)

	for {
		val, err := parseRESP(reader)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
			} else {
				fmt.Println("Reading error: ", err)
			}
			break
		}

		fmt.Printf("Parsed: %+v\n", val)

		if val.Typ == "array" && len(val.Array) > 0 {
			command := strings.ToUpper(val.Array[0].Str)

			switch command {
			case "PING":
				c.Write([]byte("+PONG\r\n"))
			case "ECHO":
				if len(val.Array) < 2 {
					c.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
					continue
				}

				arg := val.Array[1].Str
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
				c.Write([]byte(resp))

			case "SET":
				if len(val.Array) < 3 {
					c.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
					continue
				}

				key := val.Array[1].Str
				valText := val.Array[2].Str

				mu.Lock()

				memory[key] = MemoryItem{
					Value: valText,
				} // RAM

				// appends all the sets to aof

				logLine := fmt.Sprintf("SET %s %s\n", key, valText) // DISK
				aofFile.WriteString(logLine)                        // DISK

				mu.Unlock()

				c.Write([]byte("+OK\r\n"))

			case "GET":
				if len(val.Array) < 2 {
					c.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
					continue
				}

				keyText := val.Array[1].Str

				mu.RLock()
				item, exists := memory[keyText]
				mu.RUnlock()

				if !exists || (!item.ExpiresIn.IsZero() && time.Now().After(item.ExpiresIn)) {
					if exists {
						mu.Lock()
						delete(memory, keyText)
						mu.Unlock()
					}

					c.Write([]byte("$-1\r\n"))
					continue
				} else {
					resp := fmt.Sprintf("$%d\r\n%s\r\n", len(item.Value), item.Value)
					c.Write([]byte(resp))
				}

			case "DEL":
				if len(val.Array) < 2 {
					c.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
					continue
				}

				numDeletions := 0

				mu.Lock()

				for i := 1; i < len(val.Array); i++ {
					keyText := val.Array[i].Str
					_, exists := memory[keyText]

					if exists {
						delete(memory, keyText)
						numDeletions++

						logLine := fmt.Sprintf("DEL %s\n", keyText)
						aofFile.WriteString(logLine)
					}
				}

				mu.Unlock()

				resp := fmt.Sprintf(":%d\r\n", numDeletions)
				c.Write([]byte(resp))

			case "EXPIRE":
				if len(val.Array) < 3 {
					c.Write([]byte("-ERR wrong number of arguments for 'expire' command\r\n"))
					continue
				}

				keyText := val.Array[1].Str
				secondsStr := val.Array[2].Str

				seconds, err := strconv.Atoi(secondsStr)

				if err != nil {
					c.Write([]byte("-ERR value is not an integer or out of range\r\n"))
					continue
				}

				mu.Lock()
				item, exists := memory[keyText]

				if exists {
					item.ExpiresIn = time.Now().Add(time.Duration(seconds) * time.Second)
					memory[keyText] = item

					logLine := fmt.Sprintf("EXPIRE %s %s\n", keyText, secondsStr)
					aofFile.WriteString(logLine)

					mu.Unlock()
					c.Write([]byte(":1\r\n"))
				} else {
					mu.Unlock()
					c.Write([]byte(":0\r\n"))
				}

			case "PUBLISH":
				if len(val.Array) < 3 {
					c.Write([]byte("-ERR wrong number of arguments for 'publish' command\r\n"))
					continue
				}

				topic := val.Array[1].Str
				message := val.Array[2].Str

				pubsubMu.RLock()
				subscribers := pubsub[topic]
				pubsubMu.RUnlock()

				counter := 0

				for _, channel := range subscribers {
					channel <- message
					counter++
				}

				resp := fmt.Sprintf(":%d\r\n", counter)
				c.Write([]byte(resp))

			case "SUBSCRIBE":
				if len(val.Array) < 2 {
					c.Write([]byte("-ERR wrong number of arguments for 'subscribe' command\r\n"))
					continue
				}

				topic := val.Array[1].Str

				myChannel := make(chan string)

				pubsubMu.Lock()
				pubsub[topic] = append(pubsub[topic], myChannel)
				pubsubMu.Unlock()

				defer func() {
					pubsubMu.Lock()
					subscribers := pubsub[topic]

					for i, channel := range subscribers {
						if channel == myChannel {
							pubsub[topic] = append(subscribers[:i], subscribers[i+1:]...)
							break
						}
					}

					pubsubMu.Unlock()
					fmt.Printf("Client removed from topic '%s'\n", topic)
					close(myChannel)
				}()

				go func() {
					for receivedMessage := range myChannel {
						respMsg := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(topic), topic, len(receivedMessage), receivedMessage)
						_, err := c.Write([]byte(respMsg))
						if err != nil {
							break
						}
					}
				}()

			case "BGREWRITEAOF":
				go rewriteAOF()

				c.Write([]byte("+Background append only file rewriting started\r\n"))

			default:
				respErr := fmt.Sprintf("-ERR unknown command '%s' \r\n", command)
				c.Write([]byte(respErr))
			}
		}
	}

}

func parseRESP(reader *bufio.Reader) (RESPValue, error) {
	t, err := reader.ReadByte()
	if err != nil {
		return RESPValue{}, err
	}

	switch t {
	case '*':
		return parseArray(reader)
	case '$':
		return parseBulk(reader)
	case '+':
		return parseString(reader)
	default:
		return RESPValue{}, fmt.Errorf("Unknown type: %c", t)
	}
}

func parseArray(reader *bufio.Reader) (RESPValue, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return RESPValue{}, err
	}

	size, err := strconv.Atoi(string(line[:len(line)-2]))
	if err != nil {
		return RESPValue{}, err
	}

	items := make([]RESPValue, 0, size)

	for i := 0; i < size; i++ {
		val, err := parseRESP(reader)
		if err != nil {
			return RESPValue{}, err
		}

		items = append(items, val)
	}

	return RESPValue{
		Typ:   "array",
		Array: items,
	}, nil
}

func parseBulk(reader *bufio.Reader) (RESPValue, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return RESPValue{}, err
	}

	size, _ := strconv.Atoi(string(line[:len(line)-2]))

	word := make([]byte, size)

	io.ReadFull(reader, word)

	reader.ReadByte() // \r
	reader.ReadByte() // \n

	return RESPValue{
		Typ: "bulk",
		Str: string(word),
	}, nil
}

func parseString(reader *bufio.Reader) (RESPValue, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return RESPValue{}, err
	}
	text := string(line[:len(line)-2])
	return RESPValue{
		Typ: "string",
		Str: text,
	}, nil
}

func loadAOF() {
	file, err := os.Open("database.aof")
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Println("Error while trying to read AOF: ", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	loadedCommands := 0

	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Fields(line)

		command := words[0]

		if command == "SET" && len(words) >= 3 {
			key := words[1]
			val := strings.Join(words[2:], " ")

			memory[key] = MemoryItem{
				Value: val,
			}

			loadedCommands++
		} else if command == "DEL" && len(words) >= 2 {
			key := words[1]
			delete(memory, key)
			loadedCommands++
		} else if command == "EXPIRE" && len(words) >= 3 {
			key := words[1]
			secondsStr := words[2]

			seconds, err := strconv.Atoi(secondsStr)
			if err == nil {
				item, exists := memory[key]
				if exists {
					item.ExpiresIn = time.Now().Add(time.Duration(seconds) * time.Second)
					memory[key] = item
					loadedCommands++
				}
			}
		}
	}

	log.Printf("AOF loaded successfully. %d commands loaded to the memory.\n", loadedCommands)
}

func backgroundSync() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C

		if aofFile != nil {
			aofFile.Sync()
		}
	}
}

func rewriteAOF() {
	log.Println("Compacting AOF")

	mu.RLock()

	tempFile, err := os.OpenFile("temp.aof", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)

	if err != nil {
		log.Println("Error while creating temp.aof:", err)
		mu.RUnlock()
		return
	}

	writtenCommands := 0
	for key, item := range memory {
		line := fmt.Sprintf("SET %s %s\n", key, item.Value)
		tempFile.WriteString(line)
		writtenCommands++

		if !item.ExpiresIn.IsZero() {
			remainingSeconds := int(time.Until(item.ExpiresIn).Seconds())

			if remainingSeconds > 0 {
				lineExpire := fmt.Sprintf("EXPIRE %s %d\n", key, remainingSeconds)
				tempFile.WriteString(lineExpire)
				writtenCommands++
			}
		}
	}
	tempFile.Close()
	mu.RUnlock()

	mu.Lock()

	if aofFile != nil {
		aofFile.Close()
	}

	os.Rename("temp.aof", "database.aof")
	aofFile, _ = os.OpenFile("database.aof", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	mu.Unlock()

	log.Printf("AOF file compacted. AOF file has %d commands\n", writtenCommands)
}
