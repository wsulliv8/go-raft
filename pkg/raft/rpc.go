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