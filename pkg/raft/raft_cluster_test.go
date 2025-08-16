package raft

import (
	"encoding/json"
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"
)

type testNode struct {
	node *Node
	srv  *rpc.Server
	ln   net.Listener
    kv   *testStore
	addr string
}

type testCluster struct {
	nodes      []testNode
	peerIndex  []map[string]int // per-node: remoteAddr -> index in node.Peers
}

// testStore is a lightweight KV used in tests (avoids module import issues)
type testStore struct {
    mu    sync.RWMutex
    store map[string]string
}

func newTestStore() *testStore { return &testStore{store: make(map[string]string)} }
func (s *testStore) Set(k, v string) { s.mu.Lock(); s.store[k] = v; s.mu.Unlock() }
func (s *testStore) Get(k string) (string, bool) { s.mu.RLock(); defer s.mu.RUnlock(); v, ok := s.store[k]; return v, ok }

func startCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{nodes: make([]testNode, n), peerIndex: make([]map[string]int, n)}
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil { t.Fatalf("listen: %v", err) }
        n := NewNode("n"+string('1'+i), ln.Addr().String())
		// initialize Raft plumbing needed by tests
        n.applyCh = make(chan ApplyMsg, 256)
        n.commitCh = make(chan struct{}, 256)
        n.demoteCh = make(chan struct{}, 1)
        n.clientRequests = make(map[int]chan CommandReply)

		srv := rpc.NewServer()
		if err := srv.RegisterName("Node", n); err != nil { t.Fatalf("register: %v", err) }
        c.nodes[i] = testNode{node: n, srv: srv, ln: ln, kv: newTestStore(), addr: ln.Addr().String()}
		c.peerIndex[i] = make(map[string]int)
		go func(s *rpc.Server, l net.Listener) {
			for {
				conn, err := l.Accept()
				if err != nil { return }
				go s.ServeConn(conn)
			}
		}(srv, ln)
	}

	// Dial peers and set Peers slice with fixed index mapping per node
	for i := 0; i < n; i++ {
		c.nodes[i].node.Peers = make([]*rpc.Client, 0, n-1)
		for j := 0; j < n; j++ {
			if i == j { continue }
			client, err := rpc.Dial("tcp", c.nodes[j].addr)
			if err != nil { t.Fatalf("dial peer: %v", err) }
			idx := len(c.nodes[i].node.Peers)
			c.nodes[i].node.Peers = append(c.nodes[i].node.Peers, client)
			c.peerIndex[i][c.nodes[j].addr] = idx
		}
		// start applier loop
        n := c.nodes[i].node
        store := c.nodes[i].kv
		go func() {
			for msg := range n.applyCh {
				var cmd struct {
					Op string
					Key string
					Value string
				}
				if err := json.Unmarshal(msg.Command, &cmd); err == nil {
					switch cmd.Op {
					case "SET":
                        store.Set(cmd.Key, cmd.Value)
					}
				}
			}
		}()
	}

	// Start nodes
	for i := 0; i < n; i++ { c.nodes[i].node.Start() }
	return c
}

func (c *testCluster) stop() {
	for i := range c.nodes {
		_ = c.nodes[i].ln.Close()
		c.nodes[i].node.Stop()
	}
}

func (c *testCluster) waitForLeader(t *testing.T, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i := range c.nodes {
			c.nodes[i].node.mu.RLock()
			isLeader := c.nodes[i].node.state == Leader
			c.nodes[i].node.mu.RUnlock()
			if isLeader { return i }
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("leader not elected within %v", timeout)
	return -1
}

func (c *testCluster) leader() int { return c.waitForLeader(&testing.T{}, 2*time.Second) }

func TestInitialLeaderElection(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stop()
	_ = c.waitForLeader(t, 3*time.Second)
}

func TestBasicCommandReplication(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stop()

	leaderIdx := c.waitForLeader(t, 3*time.Second)
	ldr := c.nodes[leaderIdx].node

	// propose SET command via leader.Command
	cmd := map[string]string{"Op":"SET", "Key":"foo", "Value":"bar"}
	bytes, _ := json.Marshal(cmd)
	var reply CommandReply
	if err := ldr.Command(&CommandArgs{Command: bytes}, &reply); err != nil || !reply.Success {
		t.Fatalf("propose failed: %v success=%v", err, reply.Success)
	}

	// wait until majority has value
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for i := range c.nodes {
			if v, ok := c.nodes[i].kv.Get("foo"); ok && v == "bar" { count++ }
		}
		if count >= 2 { break }
		time.Sleep(10 * time.Millisecond)
	}
	count := 0
	for i := range c.nodes {
		if v, ok := c.nodes[i].kv.Get("foo"); ok && v == "bar" { count++ }
	}
	if count < 2 { t.Fatalf("expected majority applied, got %d", count) }

	// read from a follower
	for i := range c.nodes {
		if i == leaderIdx { continue }
		if v, ok := c.nodes[i].kv.Get("foo"); ok && v == "bar" { return }
	}
	t.Fatalf("follower did not reflect value")
}

func TestLeaderCrashAndReelection(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stop()

	leaderIdx := c.waitForLeader(t, 3*time.Second)
	// propose one command
	cmd1 := map[string]string{"Op":"SET", "Key":"a", "Value":"1"}
	bytes1, _ := json.Marshal(cmd1)
	var r1 CommandReply
	if err := c.nodes[leaderIdx].node.Command(&CommandArgs{Command: bytes1}, &r1); err != nil || !r1.Success {
		t.Fatalf("pre-crash propose failed: %v success=%v", err, r1.Success)
	}
	// crash leader
	_ = c.nodes[leaderIdx].ln.Close()
	c.nodes[leaderIdx].node.Stop()

	// wait for new leader among remaining nodes
	deadline := time.Now().Add(4 * time.Second)
	newLeader := -1
	for time.Now().Before(deadline) {
		for i := range c.nodes {
			if i == leaderIdx { continue }
			c.nodes[i].node.mu.RLock()
			isLeader := c.nodes[i].node.state == Leader
			c.nodes[i].node.mu.RUnlock()
			if isLeader { newLeader = i; break }
		}
		if newLeader != -1 { break }
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == -1 { t.Fatalf("no new leader elected after crash") }

	// propose new command to new leader
	cmd2 := map[string]string{"Op":"SET", "Key":"b", "Value":"2"}
	bytes2, _ := json.Marshal(cmd2)
	var r2 CommandReply
	if err := c.nodes[newLeader].node.Command(&CommandArgs{Command: bytes2}, &r2); err != nil || !r2.Success {
		t.Fatalf("post-crash propose failed: %v success=%v", err, r2.Success)
	}
}

func TestNetworkPartitionMinority(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stop()

	leaderIdx := c.waitForLeader(t, 3*time.Second)

	// isolate node 2 (index 2)
	isolate := 2
	// close clients to isolated on majority
	for i := range c.nodes {
		if i == isolate { continue }
		// find client index to isolated address
		idx, ok := c.peerIndex[i][c.nodes[isolate].addr]
		if ok {
			_ = c.nodes[i].node.Peers[idx].Close()
		}
	}
	// close isolated's clients to majority
	for idx := range c.nodes[isolate].node.Peers {
		_ = c.nodes[isolate].node.Peers[idx].Close()
	}

	// Send a command to majority leader and expect commit
	cmd := map[string]string{"Op":"SET", "Key":"p", "Value":"q"}
	bytes, _ := json.Marshal(cmd)
	var r CommandReply
	if err := c.nodes[leaderIdx].node.Command(&CommandArgs{Command: bytes}, &r); err != nil || !r.Success {
		t.Fatalf("majority propose failed: %v success=%v", err, r.Success)
	}

	// Attempt to send a command to isolated node; expect redirect/failure (not leader)
	var rIso CommandReply
	_ = c.nodes[isolate].node.Command(&CommandArgs{Command: bytes}, &rIso)
	if rIso.Success {
		t.Fatalf("expected isolated propose to fail or redirect")
	}

	// heal: re-dial clients to isolated
	for i := range c.nodes {
		if i == isolate { continue }
		client, err := rpc.Dial("tcp", c.nodes[isolate].addr)
		if err != nil { t.Fatalf("heal dial: %v", err) }
		idx := c.peerIndex[i][c.nodes[isolate].addr]
		c.nodes[i].node.Peers[idx] = client
	}
	for j := range c.nodes[isolate].node.Peers {
		// re-dial majority peer j by address order from peerIndex[isolate]
		// since we appended peers in order, we can rebuild
		_ = c.nodes[isolate].node.Peers[j].Close()
	}
	// rebuild isolated's peers list
	c.nodes[isolate].node.Peers = nil
	for j := range c.nodes {
		if j == isolate { continue }
		client, err := rpc.Dial("tcp", c.nodes[j].addr)
		if err != nil { t.Fatalf("heal back dial: %v", err) }
		c.nodes[isolate].node.Peers = append(c.nodes[isolate].node.Peers, client)
	}

	// wait for isolated to catch up on key p
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := c.nodes[isolate].kv.Get("p"); ok && v == "q" { return }
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("isolated node did not catch up after heal")
}


