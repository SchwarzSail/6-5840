package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"github.com/sasha-s/go-deadlock"
	"sync"

	//	"bytes"
	"math/rand"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
	//For Lab4:
	TermUpdated bool
	//for lab5
	CurrentTerm int
}

type State int

const (
	Leader State = iota
	Candidate
	Follower
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).

	state State

	// state a Raft server must maintain.
	//Persistent state on all servers:
	//(Updated on stable storage before responding to RPCs)
	currentTerm int
	//currentTerm latest term server has seen (initialized to 0
	//on first boot, increases monotonically)
	votedFor int
	//votedFor candidateId that received vote in current
	//term (or null if none)

	log []LogEntry
	//log[] log entries; each entry contains command
	//for state machine, and term when entry
	//was received by leader (first index is 1)

	//Volatile state on all servers:
	commitIndex int
	//commitIndex index of highest log entry known to be
	//committed (initialized to 0, increases
	//monotonically)
	lastApplied int
	//lastApplied index of highest log entry applied to state
	//machine (initialized to 0, increases
	//monotonically)

	//Volatile state on leaders:
	//(Reinitialized after election)
	nextIndex []int
	//nextIndex[] for each server, index of the next log entry
	//to send to that server (initialized to leader
	//last log index + 1)
	matchIndex []int
	//matchIndex[] for each server, index of highest log entry
	//known to be replicated on server
	//(initialized to 0, increases monotonically)

	applyCh chan ApplyMsg

	lastHeartBestsTime time.Time
	lastElectionTime   time.Time

	cond *sync.Cond //用于启动goroutine

	//If, when the server comes back up, it reads the updated snapshot, but the outdated log,
	//it may end up applying some log entries that are already contained within the snapshot.
	//This happens since the commitIndex and lastApplied are not persisted,
	//and so Raft doesn’t know that those log entries have already been applied.
	//The fix for this is to introduce a piece of persistent state to Raft that records
	//what “real” index the first entry in Raft’s persisted log corresponds to.
	//his can then be compared to the loaded snapshot’s lastIncludedIndex to determine what elements at the head of the log to discard.
	lastIncludedIndex int //snapshot的边界
	snapshot          []byte
	applyingSnapshot  bool //用于应用层标识是否应用snapshot
	termUpdated       bool //标识term改变，并告知应用层
}

func (rf *Raft) stateChanged(state State) {
	defer rf.persist()
	rf.resetElectionTimer()
	rf.resetHeartBeatTimer()
	rf.state = state
	switch rf.state {
	case Leader:
		DPrintf("---------------Sever %d becomes leader at term %d-----------------", rf.me, rf.currentTerm)
		//更新nextIndex和matchIndex
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for peer := range rf.peers {
			rf.nextIndex[peer] = rf.lastLogIndex() + 1
			rf.matchIndex[peer] = 0
		}
		go rf.sendHeartBeats(rf.currentTerm)
	case Candidate:
		DPrintf("Server %d becomes candidate", rf.me)
		rf.currentTerm++
		rf.votedFor = rf.me
	case Follower:
		DPrintf("Server %d becomes follower", rf.me)
		rf.votedFor = -1
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
	} else {
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Command: command,
		})
		index = rf.lastLogIndex()
		Debug(dLeader, "Leader %d append the log whose index is %d, and term is %d", rf.me, index, term)
		go rf.quicklySync() //发起快速同步日记请求(lab4A)
		rf.persist()
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	if z == 1 {
		Debug(dDrop, "Server %d is broken out", rf.me)
	}
	return z == 1
}

// for leader election
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		// Your code here (3A)
		// Check if a leader election should be started.
		//开启领导者选举的两个条件
		switch rf.state {
		case Follower: //1. follower的心跳检测定时器超时
			//If election timeout elapses without receiving AppendEntries
			//RPC from current leader or granting vote to candidate:
			//convert to candidate
			if time.Since(rf.lastElectionTime) > rf.electionTimeout() && (rf.votedFor == -1 || time.Since(rf.lastHeartBestsTime) > rf.heartBeatTimeOut()) {
				Debug(dClient, "Server %d find the heartbeats timeout", rf.me)
				rf.stateChanged(Candidate)
				rf.resetElectionTimer()
				go rf.startElection(rf.currentTerm)
			}
		case Candidate: //2. candidate的选举时间超时
			if time.Since(rf.lastElectionTime) > rf.electionTimeout() {
				Debug(dClient, "Server %d find the election is timeout and previous term is %d", rf.me, rf.currentTerm)
				rf.stateChanged(Candidate)
				go rf.startElection(rf.currentTerm)
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyTermUpdated() {
	for !rf.killed() {
		rf.mu.Lock()
		for !rf.termUpdated {
			rf.cond.Wait()
		}
		if rf.termUpdated {
			rf.termUpdated = false
			msg := ApplyMsg{
				CurrentTerm: rf.currentTerm,
				TermUpdated: true,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) apply() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.cond.Wait()
		}
		if rf.applyingSnapshot {
			rf.applyingSnapshot = false
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.log[rf.logIndex(rf.lastIncludedIndex)].Term,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			Debug(dSnap, "Server %d install the snapshot and the snapshotIndex is %d", rf.me, msg.SnapshotIndex)
			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, msg.SnapshotIndex)
			rf.mu.Unlock()
			continue
		}
		if rf.commitIndex > rf.lastApplied {
			//If commitIndex > lastApplied: increment lastApplied, apply
			//log[lastApplied] to state machine (§5.3)
			msgs := make([]ApplyMsg, 0)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.logIndex(i)].Command,
					CommandIndex: i,
				})
			}
			rf.mu.Unlock()

			for _, msg := range msgs {
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied = msg.CommandIndex
				//Debug(dCommit,"Server %d apply the log whose index is %d, and term is %d", rf.me, msg.CommandIndex, rf.log[rf.logIndex(msg.CommandIndex)].Term)
				rf.mu.Unlock()

			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.log = make([]LogEntry, 1)
	rf.mu = deadlock.Mutex{}
	rf.cond = sync.NewCond(&rf.mu)
	rf.votedFor = -1
	rf.state = Follower
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastElectionTime = time.Now()
	rf.lastHeartBestsTime = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.apply()
	go rf.applyTermUpdated()
	return rf
}
