package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"raft/internal/raft"
	"strconv"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano()) // 一定要加！！随机超时才正常！

	if len(os.Args) != 3 {
		log.Fatalf("usage: server <id> <port>")
	}

	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("invalid id: %v", err)
	}
	port := os.Args[2]

	peers := []string{
		"localhost:8001",
		"localhost:8002",
		"localhost:8003",
		"localhost:8004",
		"localhost:8005",
	}

	r := raft.NewRaft(id-1, peers) // 创建 Raft 实例

	rpc.Register(r) // 注册 RPC 服务

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}

	fmt.Printf("Server %d listening on port %s\n", id, port)

	for {
		// 接受连接
		conn, err := listener.Accept()
		if err != nil {
			log.Println("accept error:", err)
			continue
		}
		go rpc.ServeConn(conn) // 处理连接
	}
}
