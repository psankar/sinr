package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

type sinr struct {
	data map[string]string
	lock sync.RWMutex
}

func (s *sinr) Set(key, val string) {
	s.lock.Lock()
	s.data[key] = val
	s.lock.Unlock()
}

func (s *sinr) Get(key string) string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.data[key]
}

type client struct {
	ID     int64
	Conn   net.Conn
	Reader *bufio.Reader
	Sinr   *sinr
}

func (c *client) Serve() {
	log.Println("Accepted Connection: ", c.Conn.RemoteAddr().String())
	defer c.Conn.Close()

	c.sendLine("+OK\r\n")

	c.Reader = bufio.NewReader(c.Conn)

	for {
		command, err := getCommand(c.Reader)
		if err != nil {

			if err == io.EOF {
				log.Println("Closing Connection: ", c.Conn.RemoteAddr().String())
				return
			}
			log.Println("ERROR: ", err)
			continue
		}

		switch command.ID {
		case GET:
			log.Println("GET Command")
			c.sendLine("+OK\r\n")
		case SET:
			log.Println("SET Command")
			c.sendLine("+OK\r\n")
		case QUIT:
			log.Println("QUIT Command")
			c.sendLine("+OK\r\n")
			return
		}
	}
}

func (c *client) sendLine(line string) {
	if _, err := io.WriteString(c.Conn, line); err != nil {
		log.Println("ERROR: while sendLine():", err)
	}
}

type command struct {
	ID   string
	Args []string
}

func getCommand(r *bufio.Reader) (*command, error) {

	line, err1 := readLine(r)
	if err1 != nil {
		return nil, err1
	}

	if !strings.HasPrefix(line, "*") {
		return &command{ID: line}, nil
	}

	// *n arguments ARRAY
	argc, parseErr := strconv.ParseUint(line[1:], 10, 64)
	if parseErr != nil || argc < 1 {
		return nil, fmt.Errorf("Invalid number of arguments: %s", line)
	}

	args := make([]string, 0, argc)
	for i := 0; i < int(argc); i++ {
		l2, err2 := readLine(r)
		if err2 != nil {
			return nil, err2
		}

		if !strings.HasPrefix(l2, "$") {
			return nil, fmt.Errorf("Invalid command: %s", l2)
		}

		// $ BULK STRINGS
		bytesToRead, bulkErr := strconv.ParseUint(l2[1:], 10, 64)
		if bulkErr != nil {
			return nil, fmt.Errorf("Invalid number of bytes specified: %s", l2)
		}

		arg := make([]byte, bytesToRead+2) // 2 for CR+LF
		if _, err := io.ReadFull(r, arg); err != nil {
			return nil, err
		}

		args = append(args, string(arg[0:len(arg)-2]))
	}

	return &command{ID: args[0], Args: args[1:]}, nil
}

func readLine(r *bufio.Reader) (string, error) {
	line := ""

	for {
		b, isPrefix, err := r.ReadLine()
		if err != nil {
			return "", err
		}

		line += string(b)
		if isPrefix {
			continue
		}

		return line, nil
	}
}

const (
	GET  string = "GET"
	SET         = "SET"
	QUIT        = "QUIT"
)

func main() {
	log.Println("Starting sinr server")
	addr := ":15000"
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Error starting sinr server", err)
		return
	}

	log.Println("Accepting connections at", addr)

	var clientID int64
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err)
			continue
		}

		s := &sinr{
			data: make(map[string]string),
		}

		clientID++
		client := &client{ID: clientID, Conn: conn, Sinr: s}
		go client.Serve()
	}
}
