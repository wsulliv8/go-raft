package raft

import (
	"log"
	"sync"
	"time"
	"math/rand"
	"net/rpc"
	"github.com/wsulliv8/go-raft/pkg/kvstore"
)

type State int

const (
	Follower State = iota
	Leader
	Candidate
)

// Node state struct derived from the Raft paper
type Node struct {
	Id string
	Addr string
	mu sync.RWMutex
	wg sync.WaitGroup
	Peers []*rpc.Client // RPC clients to all other servers

	// Persistent state (updated on stable storage before responding to RPCs)
	// TODO: IMPLEMENT PERSISTENT STATE LATER
	currentTerm int // Current term number, increases monotonically
	votedFor string // Candidate that received vote in current term
	log []LogEntry // Log of commands for state machine (key value store)

	// Volatile state on all servers
	state State // Follower, Leader, Candidate
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// Volatile state on leaders (reinitialized after election)
	nextIndex []int // Index of next log entry to send to each server
	matchIndex []int // Index of highest log entry known to be replicated on each server

	// Timing variables
	electionTimeout time.Duration // Time to wait before starting an election
	heartbeatTimeout time.Duration // Time between heartbeats
	electionTimer *time.Timer
	heartbeatTimer *time.Timer
	stopCh chan struct{} // Channel to signal stop

	// Vote tracking
	// Vote channel carries voterId to ensure idempotency and prevent duplicate votes
	votesCh chan struct {
		granted bool
		voterId string
	} // Channel to count votes
	votes map[string]bool // Number of votes received - ensure idemptotent
	demoteCh chan struct{} // Channel to signal demotion to follower

	// Client communication
	currentLeader string // Followers route client messages to leader
	commitCh chan struct{} // Channel to send commit index to state machine
	clientRequests map[int]chan CommandReply // Track client IDs and response channels 

	// State machine
	kvstore *kvstore.KVStore
	applyCh chan LogEntry // Channel to send committed log entries to state machine
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
		kvstore: kvstore.NewKVStore(),
		applyCh: make(chan LogEntry),
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
	for _, peer := range n.Peers {
		if peer != nil {
			peer.Close()
		}
	}
	log.Printf("Node %s stopped", n.Id)
}

func (n *Node) startElection() {
	// Get last log index/term and prepare arguments for RPC
	lastLogIndex, lastLogTerm := 0,0
	if len(n.log) > 0 {
		lastLogIndex = len(n.log) - 1
		lastLogTerm = n.log[lastLogIndex].Term
	}
	args := RequestVoteArgs{
		Term: n.currentTerm,
		CandidateId: n.Id,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}

	for _, peer := range n.Peers { // Send RequestVote RPCs to all other servers 
		go func(peer *rpc.Client) {
			var reply RequestVoteReply
			if err := peer.Call("Node.RequestVote", args, &reply); err != nil {
				log.Printf("Error sending RequestVote to %s: %v", peer, err)
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.mu.Unlock()
				n.demoteCh <- struct{}{}
				return
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				n.votesCh <- struct { granted bool; voterId string } { granted: true, voterId: args.CandidateId }
			} else {
				n.votesCh <- struct { granted bool; voterId string } { granted: false, voterId: args.CandidateId }
			}
		}(peer)
	}
}

// Parse command from log entry and apply to state machine
func (n *Node) applyLogEntry(entry LogEntry) {
	command := string(entry.Command)
	parts := strings.Split(command, " ")

	if len(parts) < 2 {
		log.Printf("Invalid command: %s", command)
		return
	}

	op := parts[0]
	key := parts[1]
	value := ""

	if len(parts) > 2 {
		value = parts[2]
	}

	switch op {
	case "SET":
		n.kvstore.Set(key, value)
	case "GET":
		value, err := n.kvstore.Get(key)
		if err != nil {
			log.Printf("Error getting value for key %s: %v", key, err)
		}
	default:
		log.Printf("Invalid operation: %s", op)
	}
}

// Main Event Loop
func (n *Node) run() {
	n.wg.Add(1)
	defer n.wg.Done()

	// Initialize timers
	n.electionTimeout = time.Duration(150 + rand.Intn(150)) * time.Millisecond
	n.electionTimer = time.NewTimer(n.electionTimeout)
	n.heartbeatTimeout = time.Duration(50) * time.Millisecond
	n.heartbeatTimer = time.NewTimer(n.heartbeatTimeout)
	defer n.electionTimer.Stop()
	defer n.heartbeatTimer.Stop()
	
	for {
		switch n.state {

			case Follower:
				select {
					case <- n.stopCh:
						return
					// Election timeout
					case <-n.electionTimer.C:
						// Increment current term, become candidate, and vote for self
						n.mu.Lock()
						log.Printf("Node %s starting election", n.Id)
						n.currentTerm++
						n.votedFor = n.Id
						n.state = Candidate
						n.votes = make(map[string]bool)
						n.votes[n.Id] = true
						n.votesCh = make(chan struct { granted bool; voterId string }, len(n.Peers)) // Buffer channel to avoid blocking
						go n.startElection() // Start election in background
						n.mu.Unlock()
						n.electionTimer.Reset(n.electionTimeout)
					}

			case Candidate:
				select {
					case <- n.stopCh:
						return
					// Election timeout
					case <-n.electionTimer.C:
						// Increment current term, become candidate, and vote for self
						n.mu.Lock()
						log.Printf("Node %s starting election", n.Id)
						n.currentTerm++
						n.votedFor = n.Id
						n.state = Candidate
						n.votes = make(map[string]bool)
						n.votes[n.Id] = true
						n.votesCh = make(chan struct { granted bool; voterId string }, len(n.Peers)) // Buffer channel to avoid blocking
						go n.startElection() // Start election in background
						n.mu.Unlock()
						n.electionTimer.Reset(n.electionTimeout)
					// Vote received
					case vote := <-n.votesCh:
						n.mu.Lock()
						if vote.granted {
							if _, ok := n.votes[vote.voterId]; !ok {
								n.votes[vote.voterId] = true
								log.Printf("Node %s received vote from %s", n.Id, vote.voterId)
							}
							// If received majority of votes, become leader
							if len(n.votes) > len(n.Peers)/2 {
								log.Printf("Node %s won election. Becoming leader.", n.Id)
								// Initialize Leader state
								n.state = Leader
								n.nextIndex = make([]int, len(n.Peers))
								n.matchIndex = make([]int, len(n.Peers))
								for i := range n.Peers {
									n.nextIndex[i] = len(n.log)
									n.matchIndex[i] = 0
								}
								if !n.electionTimer.Stop() { select { case <-n.electionTimer.C: default: } } // If Stop failed, read channel to avoid blocking
								
								// Leader sends initial heartbeats to followers
								n.heartbeatTimer.Reset(0) // Fire immediately

							} else {
								log.Printf("Node %s lost election. Becoming follower.", n.Id)
								n.state = Follower
								n.votedFor = ""
								n.electionTimer.Reset(n.electionTimeout)
							}
						}
						n.mu.Unlock()
						case <- n.demoteCh:
							n.mu.Lock()
							log.Printf("Node %s demoted to follower", n.Id)
							n.state = Follower
							n.votedFor = ""
							n.mu.Unlock()
							n.electionTimer.Reset(n.electionTimeout)
				}

			case Leader:
				// TODO: Implement receive message action
				// TODO: What if a node other than leader receives message? Need to send to leader
				select {
					case <- n.stopCh:
						return
					case <- n.heartbeatTimer.C:
						// Send heartbeats to followers
						for i := range n.Peers {
							n.sendAppendEntries(i)
						}
						n.heartbeatTimer.Reset(n.heartbeatTimeout)
					case <- n.demoteCh:
						n.mu.Lock()
						log.Printf("Node %s demoted to follower", n.Id)
						n.state = Follower
						n.votedFor = ""
						n.mu.Unlock()
						n.electionTimer.Reset(n.electionTimeout)
					case <- n.commitCh:
						n.mu.Lock()
						for n.lastApplied < n.commitIndex {
							n.lastApplied++
							entry := n.log[n.lastApplied]
							
							// Send command to state machine
							n.applyCh <- entry
							log.Printf("Node %s applied log entry %d", n.Id, n.lastApplied)

							// If this was a client request, send response
							if respCh, ok := n.clientRequests[n.lastApplied]; ok {
								respCh <- CommandReply{
									Success: true,
									LeaderId: n.Id,
									CurrentTerm: n.currentTerm,
								}
								delete(n.clientRequests, n.lastApplied)
							}
						}
						n.mu.Unlock()
				// Apply log entry to state machine - applicable to all states
				case entry := <- n.applyCh:
					n.mu.Lock()

					if entry.Index == n.lastApplied + 1 {
						n.applyLogEntry(entry)
						n.lastApplied++
					}
					
					// If this is a client request, send response
					if respCh, ok := n.clientRequests[entry.Index]; ok {
						respCh <- CommandReply{
							Success: true,
							LeaderId: n.Id,
							CurrentTerm: n.currentTerm,
						}
						delete(n.clientRequests, entry.Index)
					}
					n.mu.Unlock()
				}
			}
		}
}

