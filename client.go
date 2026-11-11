package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

const (
	host = "127.0.0.1"
	port = "8080"
)

func main() {
	c, err := net.Dial("tcp", host+":"+port)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	s := bufio.NewScanner(os.Stdin)
	go writeMessage(s, c)

	tmp := make([]byte, 4096)
	for {
		size, err := c.Read(tmp)
		if err != nil {
			log.Printf("Error while receiving message: %s\n", err)
			return
		}
		log.Printf("Resposta: %s\n", string(tmp[:size]))
	}

}

func writeMessage(s *bufio.Scanner, c net.Conn) {
	for s.Scan() {
		text := s.Text()

		if text == "" {
			continue
		}

		words := strings.Fields(text)

		packetRESP := fmt.Sprintf("*%d\r\n", len(words))

		for _, word := range words {
			packetRESP += fmt.Sprintf("$%d\r\n%s\r\n", len(word), word)
		}

		c.Write([]byte(packetRESP))
	}
}
