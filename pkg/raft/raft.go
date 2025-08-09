package raft

import (
	"log"
	"sync"
	"time"
	"math/rand"
	"net/rpc"
)

// Node state struct derived from the Raft paper
type Node struct {
	Id string
	Addr string
	mu sync.RWMutex
	Peers []*rpc.Client // RPC clients to all other servers

	// Persistent state (updated on stable storage before responding to RPCs)
	// IMPLEMENT PERSISTENT STATE LATER
	currentTerm int // Current term number, increases monotonically
	votedFor string // Candidate that received vote in current term
	log []LogEntry // Log of commands for state machine (key value store)

	// Volatile state on all servers
	state State // Follower, Leader, Candidate
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// Volatile state on leaders (reinitialized after election)
	// TODO: Set these for leader after election
	nextIndex []int
	matchIndex []int

	// Timing variables
	electionTimeout time.Duration // Time to wait before starting an election
	heartbeatTimeout time.Duration // Time between heartbeats
	heartbeatCh chan struct{} // Channel to signal heartbeat
	stopCh chan struct{} // Channel to signal stop
	wg sync.WaitGroup
}

type VoteCounter struct {
	mu sync.Mutex
	count int
}

func NewNode(id string, addr string) *Node {
	return &Node{
		Id: id,
		Addr: addr,
		state: Follower,
		currentTerm: 0,
		votedFor: "",
		log: []LogEntry{},
		commitIndex: 0,
		electionTimeout: time.Duration(randElection(150)) * time.Millisecond,
		heartbeatTimeout:	time.Duration(50) * time.Millisecond,
	}
}

func (n *Node) Start() {
	log.Printf("Starting node %s", n.Id)

	n.mu.Lock()
	n.state = Follower
	n.stopCh = make(chan struct{})
	n.mu.Unlock()

	n.wg.Add(1)
	go n.run()
}

func (n *Node) Stop() {
	log.Printf("Stopping node %s", n.Id)

	// Gracefully stop the node
	close(n.stopCh)
	n.wg.Wait()
	for _, peer := range n.peers {
		if peer != nil {
			peer.Close()
		}
	}
	log.Printf("Node %s stopped", n.Id)
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() { select { case <-t.C: default: } }
	t.Reset(d)
}

func randElection(base time.Duration) time.Duration {
	return base + time.Duration(rand.Intn(int(base)))
}

func (vc *VoteCounter) AddVote() int {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.count++
	return vc.count
}

func (n *Node) startElection() {
	wg := sync.WaitGroup{}
	vc := VoteCounter{}
	for _, peer := range n.peers { // Send RequestVote RPCs to all other servers 
		go func(peer string) {
			wg.Add(1)
			reply := RequestVoteReply{}
			args := RequestVoteArgs{
				Term: n.currentTerm,
				CandidateId: n.Id,
				LastLogIndex: len(n.log) - 1,
				LastLogTerm: n.log[len(n.log)-1].Term,
			}
			if err := peer.RequestVote(args, &reply); err != nil {
				log.Printf("Error sending RequestVote to %s: %v", peer, err)
			}
			if reply.VoteGranted {
				vc.AddVote()
			}
			if reply.Term > n.currentTerm {
				n.mu.Lock()
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
				n.mu.Unlock()
			}
			wg.Done()
		}(peer)
	}
	wg.Wait()
	if vc.count > len(n.peers)/2 {
		n.mu.Lock()
		n.state = Leader
		n.mu.Unlock()
	}
}

// Main Event Loop
func (n *Node) run() {
	defer n.wg.Done()
	election := time.NewTimer(n.electionTimeout)
	var heartbeat *time.Ticker

	// Cleanup 
	defer func() {
		if heartbeat != nil {
			heartbeat.Stop()
		}
		if !election.Stop() { select { case <-election.C: default: } } // If Stop failed, read the channel to avoid blocking
	}()

	for {
		switch n.state {
			case Leader: // Leader sends heartbeats to followers
				if heartbeat == nil {
					heartbeat = time.NewTicker(n.heartbeatTimeout)
				}
			default:
				if heartbeat != nil { // If leader is no longer leader, stop heartbeat
					heartbeat.Stop()
					heartbeat = nil
				}
		}
		select {
		case <-election.C: // Election timeout
			n.mu.Lock()
			n.currentTerm++
			n.votedFor = n.id
			n.state = Candidate
			n.mu.Unlock()
			n.startElection()
			resetTimer(election, randElection(n.electionTimeout))
		case <-func() <-chan time.Time { // Return heartbeat channel if leader, otherwise nil
			if heartbeat != nil { return heartbeat.C }
			return nil
		}():
			if n.state == Leader {
				n.sendHeartbeats()
			}
		case <-n.heartbeatCh:
			if n.state != Leader { // If not leader, reset election timer
				resetTimer(election, randElection(n.electionTimeout))
			}
		case <-n.stopCh:
			return
		}
	}
}
