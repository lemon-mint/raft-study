package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lemon-mint/frameio"
)

// Raft consensus algorithm for distributed systems

type Message struct {
	Type  string // "leader_heartbeat", "vote_request", "vote_response"
	State string // "leader", "follower", "candidate"
}

type State byte

const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

type Raft struct {
	state State

	lastLeaderPing int64

	connecionMap map[string]net.Conn
}

func main() {
	var port = flag.Int("port", 9090, "port to listen on")
	flag.Parse()

	//Get the cluster ip addresses from the cluster.txt file
	var cluster []string
	func() {
		file, err := os.Open("cluster.txt")
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		data, err := io.ReadAll(file)
		if err != nil {
			log.Fatal(err)
		}

		cluster = strings.Split(string(data), "\n")
	}()
	log.Println("Cluster:", cluster)

	raft := &Raft{
		state:          FOLLOWER,
		lastLeaderPing: time.Now().UnixMilli(),
	}

	// Start the Server
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		panic(err)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go func() {
			defer conn.Close()
			var msg Message
			rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
			fr, fw := frameio.NewFrameReader(rw), frameio.NewFrameWriter(rw)
			_, _ = fr, fw
			for {
				data, err := fr.Read()
				if err != nil {
					log.Println(err)
					return
				}
				json.Unmarshal(data, &msg)
				log.Println(msg)
				switch msg.Type {
				case "leader_heartbeat":
					raft.lastLeaderPing = time.Now().UnixMilli()
				}
			}
		}()
	}
}
