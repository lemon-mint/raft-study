package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lemon-mint/frameio"
)

// Raft consensus algorithm for distributed systems

type Message struct {
	Type  string // "leader_heartbeat", "vote_request", "vote_response"
	State string // "leader", "follower", "candidate"
}

type State int32

const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

type Raft struct {
	state State

	lastLeaderPing int64

	mapMutex     sync.Mutex
	connecionMap map[string]*Conn

	clusterLock sync.Mutex
	cluster     []string

	shutdownWorker chan struct{}

	electionTimeoutLock sync.Mutex
	electionTimeout     *time.Timer

	Term      int
	VotedTerm int
}

func (raft *Raft) sendVoteRequest(ip string) {
	raft.mapMutex.Lock()
	conn, ok := raft.connecionMap[ip]
	raft.mapMutex.Unlock()

	if ok {
		conn.sendVoteRequest()
	} else {
		c, err := net.Dial("tcp", ip)
		if err != nil {
			log.Printf("Error dialing %s: %s", ip, err)
			return
		}
		raft.mapMutex.Lock()
		r, w := bufio.NewReader(c), bufio.NewWriter(c)
		fr, fw := frameio.NewFrameReader(r), frameio.NewFrameWriter(w)
		raft.connecionMap[ip] = &Conn{
			conn:        c,
			reader:      r,
			writer:      w,
			FrameReader: fr,
			FrameWriter: fw,
		}
		raft.mapMutex.Unlock()
		raft.sendVoteRequest(ip)
	}
}

func (c *Conn) sendVoteRequest() {
	c.Lock()
	defer c.Unlock()
	msg := Message{
		Type:  "vote_request",
		State: "candidate",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshalling vote request: %s", err)
		return
	}
	err = c.FrameWriter.Write(data)
	if err != nil {
		log.Printf("Error writing vote request: %s", err)
		return
	}
	err = c.writer.Flush()
	if err != nil {
		log.Printf("Error flushing vote request: %s", err)
		return
	}
}

func (raft *Raft) Worker() {
	for {
		select {
		case <-raft.shutdownWorker:
			return
		case <-raft.electionTimeout.C:
			raft.electionTimeoutLock.Lock()
			if atomic.CompareAndSwapInt32((*int32)(&raft.state), int32(CANDIDATE), int32(FOLLOWER)) {
				raft.clusterLock.Lock()
				for _, ip := range raft.cluster {
					raft.sendVoteRequest(ip)
				}
				raft.clusterLock.Unlock()
			}
			raft.electionTimeoutLock.Unlock()
		}
	}
}

type Conn struct {
	sync.Mutex

	conn net.Conn

	reader *bufio.Reader
	writer *bufio.Writer

	frameio.FrameReader
	frameio.FrameWriter
}

func main() {
	var port = flag.Int("port", 9090, "port to listen on")
	flag.Parse()

	raft := &Raft{
		state:           FOLLOWER,
		lastLeaderPing:  time.Now().UnixMilli(),
		connecionMap:    make(map[string]*Conn),
		electionTimeout: time.NewTimer(time.Millisecond * time.Duration(rand.Int63n(150)+150)),
	}
	go raft.Worker()
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
	raft.cluster = cluster

	log.Println("Cluster:", cluster)

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
		log.Println("New connection:", conn.RemoteAddr())

		go func() {
			defer conn.Close()
			var msg Message
			rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
			fr, fw := frameio.NewFrameReader(rw), frameio.NewFrameWriter(rw)

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
					raft.electionTimeoutLock.Lock()
					atomic.StoreInt64(&raft.lastLeaderPing, time.Now().UnixMilli())
					raft.electionTimeout.Reset(time.Millisecond * time.Duration(rand.Int63n(150)+150))
					raft.electionTimeoutLock.Unlock()
				case "vote_request":
					raft.electionTimeoutLock.Lock()
					if atomic.LoadInt32((*int32)(&raft.state)) == int32(FOLLOWER) {
						raft.Term++
						raft.VotedTerm = raft.Term
						raft.electionTimeout.Reset(time.Millisecond * 500)
						var msg Message
						msg.Type = "vote_response"
						msg.State = "follower"
						data, err := json.Marshal(msg)
						if err != nil {
							log.Printf("Error marshalling vote response: %s", err)
							return
						}
						err = fw.Write(data)
						if err != nil {
							log.Printf("Error writing vote response: %s", err)
							return
						}
						err = rw.Flush()
						if err != nil {
							log.Printf("Error flushing vote response: %s", err)
							return
						}
					}
					raft.electionTimeoutLock.Unlock()
				}
			}
		}()
	}
}
