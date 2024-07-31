package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) heartBeatTimeOut() time.Duration {
	return time.Duration(500+(rand.Int63()%300)) * time.Millisecond
}

func (rf *Raft) resetHeartBeatTimer() {
	rf.lastHeartBestsTime = time.Now()
}

func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != Leader {
			return
		}
		rf.mu.Lock()
		for i := range rf.peers {
			if i != rf.me {
				//Snapshot
				if rf.nextIndex[i] <= rf.lastIncludedIndex {
					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.log[0].Term,
						Data:              rf.snapshot,
					}
					go rf.handleInstallSnapshot(i, args)
				} else {
					//AppendEntries
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						LeaderCommit: rf.commitIndex,
					}
					//如果该index没有存到snapshot中，且包含在leader结点的日志中
					if args.PrevLogIndex > rf.lastIncludedIndex && args.PrevLogIndex <= rf.lastLogIndex() {
						args.PrevLogTerm = rf.log[rf.logIndex(args.PrevLogIndex)].Term
					} else if args.PrevLogIndex == rf.lastIncludedIndex {
						//此时说明该index保存在snapshot中
						args.PrevLogTerm = rf.log[0].Term
					}
					args.Entries = append(make([]LogEntry,0), rf.log[rf.logIndex(rf.nextIndex[i]):]...)
					go rf.handleAppendEntries(i, args)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
