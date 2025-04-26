package raft

import (
	"fmt"
	"net/rpc"
	"time"
)

type RequestVoteArgs struct {
	Term        int // 候选人的任期号
	CandidateId int // 候选人的ID
}

type RequestVoteReply struct {
	Term        int  // 当前任期号，以便候选人更新自己的任期号
	VoteGranted bool // true 表示候选人收到了投票
}

// AppendEntriesArgs 心跳/日志同步 参数
type AppendEntriesArgs struct {
	Term     int // 领导者的任期号
	LeaderId int // 领导者的ID
}

// AppendEntriesReply 心跳/日志同步 回复
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote RPC方法
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()         // 加锁，保护共享资源的访问
	defer r.mu.Unlock() // 函数结束时解锁，确保资源被正确释放

	if args.Term < r.currentTerm { // 如果候选人的任期号小于当前任期号，拒绝投票
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// todo 后续补充更多投票逻辑

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.state = Follower // 更新状态为跟随者
		r.votedFor = -1    // 重置投票状态
	}

	if r.votedFor == -1 || r.votedFor == args.CandidateId {
		r.votedFor = args.CandidateId // 投票给候选人
		reply.VoteGranted = true
		// 打印谁给哪个候选人投票
		fmt.Printf("Node %d voted for candidate %d in term %d\n", r.id, args.CandidateId, args.Term)

		// 收到投票请求，重置选举超时
		select {
		case r.electionResetEvent <- struct{}{}:
		default:
		}
	} else {
		reply.VoteGranted = false // 拒绝投票
	}
	reply.Term = r.currentTerm // 返回当前任期号
	return nil
}

// Call  发送 RPC 请求
func Call(addr string, method string, args interface{}, reply interface{}) bool {
	client, err := rpc.Dial("tcp", addr) // 创建一个 TCP 客户端连接
	if err != nil {
		return false
	}
	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			// 处理关闭连接时的错误
			return
		}
	}(client) // 确保在函数结束时关闭连接

	call := client.Go(method, args, reply, nil) // 异步调用 RPC 方法
	select {
	case <-call.Done:
		fmt.Println("RPC call completed")
		return call.Error == nil // 返回调用结果
	case <-time.After(5 * time.Second):
		// 超时处理
		fmt.Println("RPC call timed out")
		return false
	}
}

// AppendEntries RPC方法
func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 如果收到的任期号小于当前任期号，拒绝
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return nil
	}

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.state = Follower // 更新状态为跟随者
		r.votedFor = -1    // 重置投票状态
	}

	//// 收到心跳后重置选举超时 (老Leader恢复过来后，收到新Leader就得立刻变回Follower)
	//r.currentTerm = args.Term
	//r.state = Follower // 更新状态为跟随者
	//r.votedFor = -1    // 重置投票状态

	// 非阻塞重置选举超时
	select {
	case r.electionResetEvent <- struct{}{}:
		// 重置选举超时
		//r.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	default:
	}

	reply.Success = true
	reply.Term = r.currentTerm
	fmt.Printf("Node %d received heartbeat from leader %d in term %d\n", r.id, args.LeaderId, args.Term)
	return nil
}
