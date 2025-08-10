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
	heartbeatCh chan struct{} // Channel to signal heartbeat
	stopCh chan struct{} // Channel to signal stop

	// Vote tracking
	votesCh chan bool // Channel to count votes
	votes int // Number of votes received
	demoteCh chan bool // Channel to signal demotion to follower
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
	for _, peer := range n.Peers {
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
			if reply.Term > n.currentTerm {
				n.mu.Lock()
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
				n.mu.Unlock()
			}
			if reply.VoteGranted {
				n.votesCh <- true
			} else {
				n.votesCh <- false
			}
		}(peer)
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return
	}

	log.Printf("Node %s sending heartbeats to followers", n.Id)

	for _, peer := range n.Peers {
		go func(p *rpc.Client) {
			args := AppendEntriesArgs{
				Term: n.currentTerm,
				LeaderId: n.Id,
				PrevLogIndex: n.nextIndex[i] - 1,
				PrevLogTerm: n.log[n.nextIndex[i] - 1].Term,
				Entries: n.log[n.nextIndex[i]:],
				LeaderCommit: n.commitIndex,
			}
			var reply AppendEntriesReply
			if err := p.Call("Node.AppendEntries", args, &reply); err != nil {
				log.Printf("Error sending AppendEntries to %s: %v", p, err)
				return
			}
			if reply.Term > n.currentTerm {
				n.demoteCh <- true
			}
		}(peer)
}

// Main Event Loop
func (n *Node) run() {
	n.wg.Add(1)
	defer n.wg.Done()

	// Initialize timers
	election := time.NewTimer(n.electionTimeout)
	heartbeat := time.NewTimer(n.heartbeatTimeout)
	defer heartbeat.Stop()
	defer election.Stop()
	
	for {
		switch n.state {

			case Follower:
				select {
					case <- n.stopCh:
						return
					// Election timeout
					case <-election.C:
						// Increment current term, become candidate, and vote for self
						n.mu.Lock()
						log.Printf("Node %s starting election", n.Id)
						n.currentTerm++
						n.votedFor = n.Id
						n.state = Candidate
						n.votes = 1
						n.votesCh = make(chan bool, len(n.Peers)) // Buffer channel to avoid blocking
						go n.startElection() // Start election in background
						n.mu.Unlock()
						resetTimer(election, randElection(n.electionTimeout))
				}

			case Candidate:
				select {
					case <- n.stopCh:
						return
					// Election timeout
					case <-election.C:
						// Increment current term, become candidate, and vote for self
						n.mu.Lock()
						log.Printf("Node %s starting election", n.Id)
						n.currentTerm++
						n.votedFor = n.Id
						n.state = Candidate
						n.votes = 1
						n.votesCh = make(chan bool, len(n.Peers)) // Buffer channel to avoid blocking
						go n.startElection() // Start election in background
						n.mu.Unlock()
						resetTimer(election, randElection(n.electionTimeout))
					// Vote received
					case vote := <-n.votesCh:
						n.mu.Lock()
						if vote {
							n.votes++
							if n.votes > len(n.Peers)/2 {
								log.Printf("Node %s won election. Becoming leader.", n.Id)
								// Initialize Leader state
								n.state = Leader
								n.nextIndex = make([]int, len(n.Peers))
								n.matchIndex = make([]int, len(n.Peers))
								for i := range n.Peers {
									n.nextIndex[i] = len(n.log)
									n.matchIndex[i] = 0
								}
								if !election.Stop() { select { case <-election.C: default: } } // If Stop failed, read channel to avoid blocking
								
								// Leader sends initial heartbeats to followers
								n.sendHeartbeats() 

								// Start periodic heartbeats
								heartbeat.Reset(n.heartbeatTimeout)

							} else {
								log.Printf("Node %s lost election. Becoming follower.", n.Id)
								n.state = Follower
								n.votedFor = ""
								resetTimer(election, randElection(n.electionTimeout))
							}
						}
						n.mu.Unlock()
				}

			case Leader:
				select {
					case <- n.stopCh:
						return
					case <- heartbeat.C:
						n.sendHeartbeats()
						resetTimer(heartbeat, n.heartbeatTimeout)
					case <- n.demoteCh:
						n.mu.Lock()
						log.Printf("Node %s demoted to follower", n.Id)
						n.state = Follower
						n.votedFor = ""
						n.mu.Unlock()
						resetTimer(election, randElection(n.electionTimeout))
			}
		}
	}
}
