package raft

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

// TODO: Implement replicate log function (log request) for leader

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
	return nil
}