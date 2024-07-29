package raft

import (
	"6.5840/labgob"
	"bytes"
)

type InstallSnapshotArgs struct {
	Term int
	//term leader’s term
	LeaderId int
	//leaderId so follower can redirect clients
	LastIncludedIndex int
	//lastIncludedIndex the snapshot replaces all entries up through and including this index
	//意思是快照覆盖的日志范围
	LastIncludedTerm int
	//lastIncludedTerm term of lastIncludedIndex
	Offset int
	//offset byte offset where chunk is positioned in the snapshot file
	//快照数据块在快照文件的偏移量
	Data []byte
	//data[] raw bytes of the snapshot chunk, starting at  offset
	//原始数据块，从offset开始算起
	Done bool
	//done true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int
	//currentTerm, for leader to update itself
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Server %d readPersist failed for term %d\n", rf.me, rf.currentTerm)
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.snapshot = rf.persister.ReadSnapshot()
	DPrintf("Server %d readPersist finished\n", rf.me)

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

	//When snapshotting application state, you need to make sure that
	//the application state corresponds to the state following some known index in the Raft log.
	//This means that the application either needs to communicate to Raft what index the snapshot corresponds to,
	//or that Raft needs to delay applying additional log entries until the snapshot has been completed.
	//在这里的意思是在应用完快照后才能应用日志
	//具体实现在apply()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastIncludedIndex >= index || rf.commitIndex < index {
		return
	}
	realIndex := rf.logIndex(index)
	rf.lastIncludedIndex = index
	//将realIndex之前的日志变成快照，所以保存index+1之后的日志
	lastIncludedTerm := rf.log[realIndex].Term
	rf.log = append(make([]LogEntry, 1), rf.log[realIndex+1:]...)
	rf.log[0].Term = lastIncludedTerm
	rf.snapshot = snapshot
	rf.persist()
	DPrintf("Server %d receive a request of installing snapshot",rf.me)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.resetElectionTimer()
	rf.resetHeartBeatTimer()
	defer rf.persist()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.stateChanged(Follower)
	}
	//lab中不要求2,3,4,5
	//2. Create new snapshot file if first chunk (offset is 0)
	//3. Write data into snapshot file at given offset
	//4. Reply and wait for more data chunks if done is false
	//5. Save snapshot file, discard any existing or partial snapshot
	//with a smaller index
	reply.Term = rf.currentTerm
	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex <= rf.commitIndex{
		//DPrintf("Server %d find that there is no meaning of creating new snapshot", rf.me)
		return
	}
	//6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	for i := 1; i < len(rf.log); i++ {
		if rf.log[i].Term == args.LastIncludedTerm && rf.realIndex(i) == args.LastIncludedIndex {
			temp := make([]LogEntry, 0)
			temp = append(temp, LogEntry{Term: args.LastIncludedTerm})
			rf.log = append(temp, rf.log[i+1:]...)
			rf.commitIndex = args.LastIncludedIndex
			rf.lastApplied = args.LastIncludedIndex
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.snapshot = args.Data
			rf.applyingSnapshot = true
			DPrintf("Server %d send a install snapshot request to channel",rf.me)
			rf.cond.Broadcast()
			return
		}
	}
	//7. Discard the entire log
	temp := make([]LogEntry, 0)
	temp = append(temp, LogEntry{Term: args.LastIncludedTerm})
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.snapshot = args.Data
	rf.log = temp
	rf.applyingSnapshot = true
	DPrintf("Server %d send a install snapshot request to channel",rf.me)

	//8. Reset state machine using snapshot contents (and load
	//snapshot’s cluster configuration)
	rf.cond.Broadcast()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) handleInstallSnapshot(peer int, args InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			DPrintf("Leader %d find its term is too late", rf.me)
			rf.stateChanged(Follower)
			return
		}
		rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
		rf.nextIndex[peer] = max(rf.nextIndex[peer], args.LastIncludedIndex + 1)
	}
}
