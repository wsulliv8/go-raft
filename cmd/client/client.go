package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)

type CommandArgs struct {
	Command []byte
}

type CommandReply struct {
	Success bool
	LeaderId string
	CurrentTerm int
}


func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 4 {
		log.Fatalf("Usage: %s <server-address> <key> <value>", os.Args[0])
	}

	serverAddr := os.Args[1]
	key := os.Args[2]
	value := os.Args[3]

	command := fmt.Sprintf("SET %s %s", key, value)

	currentServer := serverAddr

	for {
		// Dial with timeout to avoid hangs
		d := net.Dialer{Timeout: 2 * time.Second}
		conn, err := d.Dial("tcp", currentServer)
		if err != nil {
			log.Printf("Failed to connect to %s: %v", currentServer, err)
			time.Sleep(1 * time.Second)
			continue
		}
		client := rpc.NewClient(conn)

		args := CommandArgs{Command: []byte(command)}
		var reply CommandReply

		log.Printf("Sending command to %s", currentServer)

		done := make(chan error, 1)
		go func() { done <- client.Call("Node.Command", &args, &reply) }()

		select {
		case err := <-done:
			client.Close()
			if err != nil {
				log.Printf("Failed to send command to %s: %v", currentServer, err)
				time.Sleep(1 * time.Second)
				continue
			}
		case <-time.After(3 * time.Second):
			client.Close()
			log.Printf("RPC to %s timed out", currentServer)
			time.Sleep(1 * time.Second)
			continue
		}

		if reply.Success {
			log.Printf("Command sent to %s", currentServer)
			break
		}

		if reply.LeaderId != "" {
			log.Printf("Redirecting to leader %s", reply.LeaderId)
			currentServer = reply.LeaderId // now an address
			continue
		}

		log.Printf("Command failed and no leader found: %v", reply)
		time.Sleep(1 * time.Second)
	}
	
}