package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) heartBeatTimeOut() time.Duration {
	return time.Duration(100+(rand.Int63()%200)) * time.Millisecond
}

func (rf *Raft) resetHeartBeatTimer() {
	rf.lastHeartBestsTime = time.Now()
}

func (rf *Raft) sendHeartBeats(term int) {
	for !rf.killed() {
		currentTerm, isLeader := rf.GetState()
		if currentTerm != term || !isLeader {
			return
		}
		rf.mu.Lock()
		for i := range rf.peers {
			if i != rf.me {
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				//Snapshot
				//说明leader想要发送的日志存在snapshot，这时候直接让follower安装snapshot
				if rf.nextIndex[i] <= rf.lastIncludedIndex {
					snapshotData := make([]byte, len(rf.snapshot))
					copy(snapshotData, rf.snapshot)
					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.log[0].Term,
						Data:              snapshotData,
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
					} else if args.PrevLogIndex > rf.lastLogIndex() {
						panic("Follower's log are longer than leader")
					}
					Debug(dTrace, "Leader %d send the append entries to server %d, and whose nextIndex[%d] is %d", rf.me, i, i, rf.nextIndex[i])
					if rf.nextIndex[i] == rf.logLength() {
						args.Entries = make([]LogEntry, 0)
					} else {
						args.Entries = append(make([]LogEntry, 0), rf.log[rf.logIndex(rf.nextIndex[i]):]...)
					}
					go rf.handleAppendEntries(i, args)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// 用于快速同步日志请求
func (rf *Raft) quicklySync() {
	rf.mu.Lock()
	for i := range rf.peers {
		if i != rf.me {
			//Snapshot
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				snapshotData := make([]byte, len(rf.snapshot))
				copy(snapshotData, rf.snapshot)
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.log[0].Term,
					Data:              snapshotData,
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
				} else if args.PrevLogIndex > rf.lastLogIndex() {
					panic("Follower's log are longer than leader")
				}
				Debug(dTrace, "Leader %d send the append entries to server %d, and whose nextIndex[%d] is %d", rf.me, i, i, rf.nextIndex[i])
				if rf.nextIndex[i] == rf.logLength() {
					args.Entries = make([]LogEntry, 0)
				} else {
					args.Entries = append(make([]LogEntry, 0), rf.log[rf.logIndex(rf.nextIndex[i]):]...)
				}
				go rf.handleAppendEntries(i, args)
			}
		}
	}
	rf.mu.Unlock()
}
