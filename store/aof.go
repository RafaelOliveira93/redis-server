package store

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var aofFile *os.File

func InitAOF() {
	loadAOF()

	var err error
	aofFile, err = os.OpenFile("database.aof", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Error while opening AOF: ", err)
	}

	go backgroundSync()
}

func AppendLog(logLine string) {
	if aofFile != nil {
		aofFile.WriteString(logLine)
	}
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
				Type:  "string",
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
		} else if command == "LPUSH" && len(words) >= 3 {
			key := words[1]
			newValue := strings.Join(words[2:], " ")

			item, exists := memory[key]
			var list []string

			if exists && item.Type == "list" {
				list = item.Value.([]string)
			} else if !exists {
				list = []string{}
			} else {
				continue
			}

			list = append([]string{newValue}, list...)

			memory[key] = MemoryItem{
				Type:      "list",
				Value:     list,
				ExpiresIn: item.ExpiresIn,
			}

			loadedCommands++
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

func RewriteAOF() {
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
		switch item.Type {

		case "string":
			lineSet := fmt.Sprintf("SET %s %s\n", key, item.Value.(string))
			tempFile.WriteString(lineSet)
			writtenCommands++

		case "list":
			list := item.Value.([]string)

			for i := len(list) - 1; i >= 0; i-- {
				line := fmt.Sprintf("LPUSH %s %s\n", key, list[i])
				tempFile.WriteString(line)
				writtenCommands++
			}
		}

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

func Close() {
	mu.Lock()
	defer mu.Unlock()

	if aofFile != nil {
		aofFile.Sync()
		aofFile.Close()
		fmt.Println("AOF closed successfully")
	}
}
