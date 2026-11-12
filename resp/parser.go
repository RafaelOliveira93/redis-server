package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type RespValue struct {
	Typ   string
	Str   string
	Num   int
	Array []RespValue
}

func ParseRESP(reader *bufio.Reader) (RespValue, error) {
	t, err := reader.ReadByte()
	if err != nil {
		return RespValue{}, err
	}

	switch t {
	case '*':
		return parseArray(reader)
	case '$':
		return parseBulk(reader)
	case '+':
		return parseString(reader)
	default:
		return RespValue{}, fmt.Errorf("Unknown type")
	}
}

func parseArray(reader *bufio.Reader) (RespValue, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return RespValue{}, err
	}

	size, err := strconv.Atoi(string(line[:len(line)-2]))
	if err != nil {
		return RespValue{}, err
	}

	items := make([]RespValue, 0, size)

	for i := 0; i < size; i++ {
		val, err := ParseRESP(reader)
		if err != nil {
			return RespValue{}, err
		}

		items = append(items, val)
	}

	return RespValue{
		Typ:   "array",
		Array: items,
	}, nil
}

func parseBulk(reader *bufio.Reader) (RespValue, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return RespValue{}, err
	}

	size, _ := strconv.Atoi(string(line[:len(line)-2]))

	word := make([]byte, size)

	io.ReadFull(reader, word)

	reader.ReadByte() // \r
	reader.ReadByte() // \n

	return RespValue{
		Typ: "bulk",
		Str: string(word),
	}, nil
}

func parseString(reader *bufio.Reader) (RespValue, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return RespValue{}, err
	}
	text := string(line[:len(line)-2])
	return RespValue{
		Typ: "string",
		Str: text,
	}, nil
}
