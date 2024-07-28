package raft

import (
	"math/rand"
	"time"
)

//需要重置选举超时器的时间结点
//1.收到leader心跳
//2.投票给candidate
//3.state发生改变

// ResetElectionTimeout 随机化选举超时时间
func (rf *Raft) electionTimeout() time.Duration {
	return time.Duration(500+(rand.Int63()%500)) * time.Millisecond
}
func (rf *Raft) resetElectionTimer() {
	rf.lastElectionTime = time.Now()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	// term candidate’s term
	CandidateID int
	//candidateId candidate requesting vote
	LastLogIndex int
	//lastLogIndex index of candidate’s last log entry (§5.4)
	LastLogTerm int
	//lastLogTerm term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	//term currentTerm, for candidate to update itself
	VoteGranted bool
	//voteGranted true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//	DPrintf("Server %d receive the request vote from Server %d", rf.me, args.CandidateID)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//1. Reply false if term < currentTerm (§5.1)
	//If a step says “reply false”, this means you should reply immediately, and not perform any of the subsequent steps
	//我的个人理解是这个请求是过期的，所以不需要任何处理，包括重置定时器
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("The Term of Candidate %d is too old, the bigger term is %d", args.CandidateID, rf.currentTerm)
		return
	}
	rf.resetElectionTimer()
	defer rf.persist()
	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.stateChanged(Follower)
	}
	//2. If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		//DPrintf("Server %d vote to Server %d", rf.me, args.CandidateID)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false

	}

	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 由candidate调用
func (rf *Raft) startElection() {
	rf.resetElectionTimer()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	vote := 1
	for i := range rf.peers {
		if i != rf.me {
			go func(peer int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, &args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					//先确保自己的状态不能有改变，不然选举没有意义
					if rf.state != Candidate || rf.currentTerm != args.Term {
						DPrintf("The requet of vote is invalid.")
						return
					}
					//If RPC request or response contains term T > currentTerm:
					//set currentTerm = T, convert to follower (§5.1)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.stateChanged(Follower)
						return
					}
					if reply.VoteGranted {
						vote++
						if vote > len(rf.peers)/2 {
							rf.stateChanged(Leader)
						}
					}
				}
			}(i)
		}
	}
}
