package raft

type State int

const (
	Follower State = iota
	Leader
	Candidate
)