package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
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

func (c client) Serve() {
	log.Println("Accepted Connection: ", c.Conn.RemoteAddr().String())
	defer c.Conn.Close()

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
		case SET:
			log.Println("SET Command")
		case QUIT:
			log.Println("QUIT Command")
			break
		}
	}

	return
}

type command struct {
	ID   string
	Args []string
}

func getCommand(r *bufio.Reader) (*command, error) {

	line := ""

	for {
		b, isPrefix, err := r.ReadLine()
		if err != nil {
			return nil, err
		}

		line += string(b)
		if isPrefix {
			continue
		}

		break
	}

	if !strings.HasPrefix(line, "*") {
		return &command{ID: line}, nil
	}

	// *n arguments
	// TODO: Parse

	return nil, fmt.Errorf("Unknown command: ", line)
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
		client := client{ID: clientID, Conn: conn, Sinr: s}
		go client.Serve()
	}
}
