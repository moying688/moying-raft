package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu          sync.Mutex // 互斥锁，用于保护共享资源的访问
	id          int        // 节点的唯一标识符
	state       State      // 当前节点的状态
	currentTerm int        // 当前任期号
	votedFor    int        // 为哪个节点投票
	log         []LogEntry // 日志条目
	// todo 后续添加

	peers              []string      // 集群中的其他节点
	electionTimeout    time.Duration // 选举超时时间
	electionResetEvent chan struct{} // 选举超时事件

	electionTimer     *time.Timer   // 选举定时器
	killElectionTimer chan struct{} // 关闭选举定时器
}

func NewRaft(id int, peers []string) *Raft {
	r := &Raft{
		id:                 id,
		state:              Follower,
		votedFor:           -1,
		peers:              peers,
		electionTimeout:    randomElectionTimeout(), // 随机 150ms-300ms
		electionResetEvent: make(chan struct{}),
		electionTimer:      time.NewTimer(randomElectionTimeout()),
		killElectionTimer:  make(chan struct{}),
	}
	r.startElectionTimer() // 启动选举定时器
	return r
}

// 设置随机超时时间 : 每个结点的超时时间 不应该一致，防止同一时刻，出现大量Candidate

func randomElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(200)) * time.Millisecond
}

// startElection 启动选举
func (r *Raft) startElectionTimer() {
	// 启动一个新的 goroutine 来处理选举超时
	go func() {
		for {
			//timeout := r.electionTimeout    // 随机超时时间
			//timer := time.NewTimer(timeout) // 创建一个新的定时器
			r.electionTimer = time.NewTimer(randomElectionTimeout())
			select {
			case <-r.electionTimer.C: // time 通道 详情
				r.startElection() // 超时 进入选举
			case <-r.electionResetEvent:
				// 收到重置事件，停止定时器
				r.electionTimer.Stop()
			case <-r.killElectionTimer:
				// 关闭选举定时器
				r.electionTimer.Stop()
				return
			}
		}
	}()
}

// startElection 启动选举
func (r *Raft) startElection() {
	r.mu.Lock()
	//defer r.mu.Unlock()

	r.state = Candidate
	r.currentTerm += 1 // 增加当前任期号
	r.votedFor = r.id  // 投票给自己
	r.mu.Unlock()

	votes := 1
	var mu sync.Mutex     // 保护 votes 变量的并发访问
	var wg sync.WaitGroup // 等待所有投票请求完成

	for i, peer := range r.peers {
		if i == r.id {
			continue // 不给自己投票
		}
		wg.Add(1) // 增加等待组计数器
		go func(peer string) {
			defer wg.Done()
			args := &RequestVoteArgs{ // 请求投票参数
				Term:        r.currentTerm,
				CandidateId: r.id,
			}
			var reply RequestVoteReply
			if Call(peer, "Raft.RequestVote", args, &reply) {
				mu.Lock()
				if reply.VoteGranted { // 如果投票成功
					votes++
				}
				mu.Unlock()
			}
		}(peer)
	}

	wg.Wait() // 等待所有投票请求完成

	r.mu.Lock() //等待所有票数都收集完后， 再次加锁
	//defer r.mu.Unlock()
	// 如果当前状态是候选人，并且获得了超过半数的投票
	if r.state == Candidate && votes > len(r.peers)/2 {
		r.state = Leader
		fmt.Printf("Node %d becomes Leader for term %d\n", r.id, r.currentTerm)
		// 启动心跳定时器
		r.startHeartbeatTimer()
		// 关闭自己的选举定时器
		close(r.killElectionTimer)
		//r.electionTimer.Stop()
	}
	r.mu.Unlock()
}

// startHeartbeatTimer 启动心跳定时器
func (r *Raft) startHeartbeatTimer() {
	go func() {
		// 之前我们写的超时时间是 300ms ~ 500ms ，因此心跳发送的频率应该更高
		ticker := time.NewTicker(50 * time.Millisecond) // 每 50ms 发送一次心跳
		defer ticker.Stop()                             // 确保在函数结束时停止定时器

		for {
			<-ticker.C
			r.mu.Lock()
			if r.state != Leader { // 如果当前不是领导者，停止发送心跳
				r.mu.Unlock()
				return
			}
			r.mu.Unlock() // 释放锁，允许其他操作

			for i, peer := range r.peers {
				if i == r.id {
					continue // 不给自己发送心跳
				}
				go func(peer string) {
					args := &AppendEntriesArgs{
						Term:     r.currentTerm,
						LeaderId: r.id,
					}
					var reply AppendEntriesReply
					Call(peer, "Raft.AppendEntries", args, &reply) // 发送心跳
				}(peer)
			}
		}
	}()
}
