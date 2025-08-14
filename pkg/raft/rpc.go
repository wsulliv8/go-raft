package raft

import "log"

type RequestVoteArgs struct {
	Term int
	CandidateId string
	LastLogIndex int
	LastLogTerm int
}

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId string
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

// Command format. Struct that can be serialized and deserialized to bytes. 
// TODO: Have client serialize struc using encoding/json and applyLogEntry deserialize back into Command
type Command struct {
	Op string
	Key string
	Value string
}

// Command RPCs are sent by clients to the leader
type CommandArgs struct {
	Command []byte
}

type CommandReply struct {
	Success bool
	LeaderId string // So follower can redirect clients
	CurrentTerm int // For leader to update itself
}

// RPC Handler for Client Requests
func (n *Node) Command(args *CommandArgs, reply *CommandReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Redirect client to leader
	if n.state != Leader {
		reply.Success = false
		reply.LeaderId = n.currentLeader
		reply.CurrentTerm = n.currentTerm
		return nil
	}

	// Append command to log
	entry := LogEntry{
		Index: len(n.log),
		Term: n.currentTerm,
		Command: args.Command,
	}
	n.log = append(n.log, entry)
	log.Printf("Node %s appended command to log", n.Id)

	// Create channel to wait for command to be committed
	commitIndex := len(n.log) - 1
	respCh := make(chan CommandReply, 1)
	n.clientRequests[commitIndex] = respCh

	// Send AppendEntries to all followers
	for i := range n.Peers {
		n.sendAppendEntries(i)
	}

	n.mu.Unlock()
	// Block and wait for response from main loop (synchronous from client's perspective)
	*reply = <-respCh
	n.mu.Lock()

	return nil
}

// Helper function for leader to send AppendEntries to all followers. Responsible for heartbeats and log replication.
func (n *Node) sendAppendEntries(peerIndex int) {
	n.mu.RLock()
	
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}

	prevLogIndex, prevLogTerm := n.nextIndex[peerIndex] - 1,-1
	if len(n.log) >= 0 {
		prevLogTerm = n.log[prevLogIndex].Term
	}

	args := AppendEntriesArgs{
		Term: n.currentTerm,
		LeaderId: n.Id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: n.log[prevLogIndex+1:],
		LeaderCommit: n.commitIndex,
	}

	n.mu.RUnlock()

	peer := n.Peers[peerIndex]
	var reply AppendEntriesReply
	go func() {
		if err := peer.Call("Node.AppendEntries", &args, &reply); err != nil {
			log.Printf("Node %s failed to send AppendEntries to %s: %v", n.Id, peer.Server(), err)
			return
		}

		n.mu.Lock()
		defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
			n.demoteCh <- struct{}{}
			return
		}

		if reply.Success {
			// Followers log is up to date, update nextIndex and matchIndex
			n.nextIndex[peerIndex] = len(n.log)
			n.matchIndex[peerIndex] = len(n.log) - 1
			
			// Check if a new entry can be committed by iterating backwards from latest entry to see if majority have it 
			for i := len(n.log) - 1; i > n.commitIndex; i-- {
				// Skip entries from previous terms
				if n.log[i].Term != n.currentTerm {
					continue
				}

				// Count number of followers that have the entry
				majorityCount := 1
				for _, match := range n.matchIndex {
					if match >= i {
						majorityCount++
					}
				}

				if majorityCount > len(n.Peers) / 2 {
					n.commitIndex = i
					n.commitCh <- struct{}{}
					break
				}
			}
		}else {
			// Followers log is not up to date, decrement nextIndex and retry
			n.nextIndex[peerIndex]--
			log.Printf("AppendEntries failed for peer %s. Decrementing nextIndex to %d and retrying.", peerIndex, n.nextIndex[peerIndex])
			n.sendAppendEntries(peerIndex)
		}
	}()
}

func (n *Node) logUpToDate(lastLogIndex int, lastLogTerm int) bool {
	followerLastLogIndex, followerLastLogTerm := 0,0
	if len(n.log) > 0 {
		followerLastLogIndex = len(n.log) - 1
		followerLastLogTerm = n.log[followerLastLogIndex].Term
	}
	return lastLogTerm > followerLastLogTerm || (lastLogTerm == followerLastLogTerm && lastLogIndex >= followerLastLogIndex)
}

func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If term is less than current term, reject vote
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// If term is greater than current term, become follower
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
	}

	// Grant vote if votedFor is empty or candidateId AND candidate's log is at least as up to date as receiver's log
	if (n.votedFor == "" || n.votedFor == args.CandidateId) && n.logUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term = n.currentTerm
		reply.VoteGranted = true
		n.votedFor = args.CandidateId
	} else {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
	}

	return nil
}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If term is less than current term, reject append entries
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return nil
	}

	// If term is greater than current term, become follower
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
	}

	n.currentLeader = args.LeaderId

	// Valid leader, reset election timer
	n.electionTimer.Reset(n.electionTimeout)

	log.Printf("Node %s received AppendEntries from leader %s", n.Id, args.LeaderId)

	// Verify log contains more entries than prefix of leader's log and the term of the last entry in prefix matches
	if args.PrevLogIndex >= 0 && (len(n.log) <= args.PrevLogIndex || n.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Term = n.currentTerm
		reply.Success = false
		return nil
	}

	// If existing entry conflicts with new entry, delete the existing entry and all that follow it
	// Append all new entries
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index < len(n.log) {
			if n.log[index].Term != entry.Term {
				n.log = n.log[:index]
				n.log = append(n.log, args.Entries[i:]...)
				break
			}
		} else {
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}

	// If leader has higher commit index (highest index known to be replicated and safe), set follower commit index
	// to highest allowable index (not to exceed the index of the last entry in the log)
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, len(n.log) - 1)
	}

	reply.Term = n.currentTerm
	reply.Success = true
	
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}