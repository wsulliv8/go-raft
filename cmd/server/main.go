package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"sync"
	"github.com/wsulliv8/go-raft/pkg/raft"
	"strconv"
	"net/rpc"
	"net"
)

// Connect to peers and start servers using separate goroutines
func connectToPeers(addresses []string) []*raft.Node {
	nodes := make([]*raft.Node, len(addresses))
	
	var serverWg sync.WaitGroup
	serverWg.Add(len(addresses))

	// Initialize nodes and start servers
	for i, addr := range addresses {
		node := raft.NewNode(strconv.Itoa(i), addr)
		nodes[i] = node

		go func(node *raft.Node) {
			defer serverWg.Done()
			
			rpc.Register(node)
			listener, err := net.Listen("tcp", node.Addr)
			if err != nil {
				log.Fatalf("Failed to listen on %s: %v", node.Addr, err)
			}
			
			go rpc.Accept(listener) // serve RPCs in background 

			log.Printf("Server %s listening on %s", node.Id, node.Addr)
		}(node)
	}

	serverWg.Wait()

	// Connect to peers
	for i, node := range nodes {
		node.Peers = make([]*rpc.Client, 0, len(addresses)-1)
		for j, peer := range nodes {
			if i == j { continue }

			client, err := rpc.Dial("tcp", peer.Addr)
			if err != nil {
				log.Fatalf("Failed to connect to %s: %v", peer.Addr, err)
			}
			node.Peers = append(node.Peers, client)
			log.Printf("Node %s connected to %s", node.Id, peer.Id)
		}
	}

	log.Println("All nodes and peers connected")

	return nodes
}

func main() {
	addresses := []string{
		"127.0.0.1:50001",
		"127.0.0.1:50002",
		"127.0.0.1:50003",
	}

	var wg sync.WaitGroup
	peers := connectToPeers(addresses)

	for _, peer := range peers {
		wg.Add(1)
		go func(peer *raft.Node) {
			defer wg.Done()
			peer.Start()
		}(peer)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Press Ctrl+C to stop")
	<-sigCh

	log.Println("Stopping nodes")
	for _, peer := range peers {
		peer.Stop()
	}
	wg.Wait()
	log.Println("All nodes stopped")
}