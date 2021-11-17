package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
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
	Type     string // "leader_heartbeat", "vote_request", "vote_response"
	State    string // "leader", "follower", "candidate"
	ReturnIP string
}

type State int32

const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

var MyIP string

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

	ReceivedVotes int
}

func (raft *Raft) sendVoteRequest(ip string) {
	log.Printf("Sending vote request to %s", ip)
	raft.mapMutex.Lock()
	conn, ok := raft.connecionMap[ip]
	raft.mapMutex.Unlock()

	if ok {
		go conn.sendVoteRequest()
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
		Type:     "vote_request",
		State:    "candidate",
		ReturnIP: MyIP,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshalling vote request: %s\n", err)
		return
	}
	err = c.FrameWriter.Write(data)
	if err != nil {
		log.Printf("Error writing vote request: %s\n", err)
		return
	}
	err = c.writer.Flush()
	if err != nil {
		log.Printf("Error flushing vote request: %s\n", err)
		return
	}
}

func (raft *Raft) sendVoteResponse(ip string) {
	raft.mapMutex.Lock()
	conn, ok := raft.connecionMap[ip]
	raft.mapMutex.Unlock()

	if ok {
		conn.sendVoteResponse()
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
		raft.sendVoteResponse(ip)
	}
}

func (c *Raft) sendLeaderHeartbeat() {
	c.mapMutex.Lock()
	for _, conn := range c.connecionMap {
		go conn.sendLeaderHeartbeat()
	}
	c.mapMutex.Unlock()
}

func (c *Conn) sendLeaderHeartbeat() {
	c.Lock()
	defer c.Unlock()
	msg := Message{
		Type:     "leader_heartbeat",
		State:    "leader",
		ReturnIP: MyIP,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshalling leader heartbeat: %s\n", err)
		return
	}
	err = c.FrameWriter.Write(data)
	if err != nil {
		log.Printf("Error writing leader heartbeat: %s\n", err)
		return
	}
	err = c.writer.Flush()
	if err != nil {
		log.Printf("Error flushing leader heartbeat: %s\n", err)
		return
	}
}

func (c *Conn) sendVoteResponse() {
	c.Lock()
	defer c.Unlock()
	msg := Message{
		Type:     "vote_response",
		State:    "follower",
		ReturnIP: MyIP,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshalling vote response: %s\n", err)
		return
	}

	err = c.FrameWriter.Write(data)
	if err != nil {
		log.Printf("Error writing vote response: %s\n", err)
		return
	}

	err = c.writer.Flush()
	if err != nil {
		log.Printf("Error flushing vote response: %s\n", err)
		return
	}
}

func (raft *Raft) Worker() {
	raft.electionTimeout.Reset(time.Millisecond * time.Duration(rand.Int63n(1500)+150))
	heartbeatTicker := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-raft.shutdownWorker:
			return
		case <-raft.electionTimeout.C:
			if raft.state == CANDIDATE {
				raft.electionTimeout.Reset(time.Millisecond * time.Duration(rand.Int63n(3000)+150))
				raft.state = FOLLOWER
				raft.Term++
				continue
			}

			log.Println("Election timeout")
			raft.electionTimeoutLock.Lock()
			if atomic.CompareAndSwapInt32((*int32)(&raft.state), int32(FOLLOWER), int32(CANDIDATE)) {
				raft.clusterLock.Lock()
				raft.ReceivedVotes = 1
				for _, ip := range raft.cluster {
					raft.sendVoteRequest(ip)
				}
				raft.clusterLock.Unlock()
			}
			raft.electionTimeout.Reset(time.Millisecond * time.Duration(rand.Int63n(1500)+150))
			raft.electionTimeoutLock.Unlock()
		case <-heartbeatTicker.C:
			if raft.state == LEADER {
				raft.sendLeaderHeartbeat()
			}
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
	MyIP = fmt.Sprintf("127.0.0.1:%d", *port)

	raft := &Raft{
		state:           FOLLOWER,
		lastLeaderPing:  time.Now().UnixMilli(),
		connecionMap:    make(map[string]*Conn),
		electionTimeout: time.NewTimer(time.Millisecond * time.Duration(rand.Int63n(150)+150)),
		Term:            -1,
	}
	if !raft.electionTimeout.Stop() {
		<-raft.electionTimeout.C
	}
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
		for i, ip := range cluster {
			cluster[i] = strings.TrimSpace(ip)
		}
	}()
	raft.cluster = cluster
	for _, ip := range cluster {
		log.Println(ip)
	}

	// Start the Server
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	go func() {
		time.Sleep(time.Second * 1)
		raft.Worker()
	}()

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
			fr, _ := frameio.NewFrameReader(rw), frameio.NewFrameWriter(rw)

			for {
				data, err := fr.Read()
				if err != nil {
					log.Println(err)
					return
				}
				json.Unmarshal(data, &msg)
				if msg.ReturnIP == MyIP {
					continue
				}
				log.Println(msg)
				switch msg.Type {
				case "leader_heartbeat":
					raft.electionTimeoutLock.Lock()
					atomic.StoreInt64(&raft.lastLeaderPing, time.Now().UnixMilli())
					raft.electionTimeout.Reset(time.Millisecond * time.Duration(rand.Int63n(1500)+1500))
					raft.electionTimeoutLock.Unlock()
				case "vote_request":
					log.Println("My state is:", raft.state)
					raft.electionTimeoutLock.Lock()
					if raft.state == FOLLOWER {
						log.Println("I am a follower")
						if raft.Term != raft.VotedTerm {
							if raft.Term == -1 {
								raft.Term = 1
							} else {
								raft.Term++
							}
							log.Println("I am voting for term:", raft.Term)
							raft.VotedTerm = raft.Term
							raft.electionTimeout.Reset(time.Millisecond * time.Duration(rand.Int63n(1500)+1500))
							go raft.sendVoteResponse(msg.ReturnIP)
							log.Println("Voted for:", conn.RemoteAddr())
						}
					}
					raft.electionTimeoutLock.Unlock()
				case "vote_response":
					log.Printf("received vote from: %s\n", conn.RemoteAddr().String())
					raft.electionTimeoutLock.Lock()
					if atomic.LoadInt32((*int32)(&raft.state)) == int32(CANDIDATE) {
						raft.ReceivedVotes++
						if raft.ReceivedVotes > len(raft.cluster)/2 {
							raft.state = LEADER
							raft.electionTimeout.Stop()
						}
					} else {
						raft.ReceivedVotes = 0
					}
					raft.electionTimeoutLock.Unlock()
				}
			}
		}()
	}
}
