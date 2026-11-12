package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"redis-server/server"
	"redis-server/store"
)

func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Usage go run . PORT(ex:8080)")
		return
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Initializing server...")
	fmt.Println("Trying to recover data from database.aof...")

	store.InitAOF()

	PORT := ":" + arguments[1]
	l, err := net.Listen("tcp4", PORT)
	if err != nil {
		log.Fatal("Error while trying to create TCP server:", err)
	}

	fmt.Printf("Redis server up on port %s\n", PORT)

	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				log.Printf("Error while trying to accept new connection: %v\n", err)
				continue
			}

			go server.HandleConnection(c)
		}
	}()

	<-sigs

	l.Close()
	store.Close()

	fmt.Println("Shutdown")
}
