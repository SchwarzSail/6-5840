package raft

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term int
	//leader’s term
	LeaderId int
	//so follower can redirect clients
	PrevLogIndex int
	//index of log entry immediately preceding new ones
	PrevLogTerm int
	//term of prevLogIndex entry
	Entries []LogEntry
	// entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int
	//leader’s commitIndex
}

type AppendEntriesReply struct {
	Term int
	//currentTerm, for leader to update itself
	Success bool
	//true if follower contained entry matching
	//prevLogIndex and prevLogTerm
	XTerm int
	//term in the conflicting entry (if any)
	XIndex int //
	//index of first entry with that term (if any)
	XLen int
	//log length
}

func (rf *Raft) lastLogIndex() int {
	//因为含有哨兵结点，所以需要-1
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 1 {
		//get from snapshot
		return rf.log[0].Term
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) firstLogIndex() int {
	if len(rf.log) == 1 {
		return rf.lastIncludedIndex
	}
	return rf.lastIncludedIndex + 1
}

//有两个概念解释清楚，因为设计到snapshot
//真实索引：指日志数组的索引，从0开始
//日志索引：包括了snapshot，数组的完整索引,从1开始计算

// 找到该index在日志数组的真实索引
func (rf *Raft) logIndex(index int) int {
	return index - rf.lastIncludedIndex
}

// 输入logIndex ,返回完整的索引
func (rf *Raft) realIndex(logIndex int) int {
	return rf.lastIncludedIndex + logIndex
}

// 数组的长度，包括了哨兵
func (rf *Raft) logLength() int {
	return rf.lastIncludedIndex + len(rf.log)
}

func (rf *Raft) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	return lastLogTerm > rf.lastLogTerm() || (lastLogTerm == rf.lastLogTerm() && lastLogIndex >= rf.lastIncludedIndex)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries handles the AppendEntries RPC call.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//if the term in the AppendEntries arguments is outdated, you should not reset your timer
	//1. Reply false if term < currentTerm (§5.1)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("Server %d have the higher term whose term is %d than leader %d whose term is %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.stateChanged(Follower)
	}

	//2. Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)
	//If you get an AppendEntries RPC with a prevLogIndex that points beyond the end of your log,
	//you should handle it the same as if you did have that entry but the term did not match (i.e., reply false).

	//follower超前，说明这个leader请求是过期的
	if rf.lastIncludedIndex > args.PrevLogIndex {
		return
	}
	defer rf.resetElectionTimer()
	defer rf.resetHeartBeatTimer()
	//follower落后太多了
	//If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = -1
		reply.XIndex = rf.logLength()
		reply.XLen = rf.logLength()
		return
	}
	//If a follower does have prevLogIndex in its log, but the term does not match,
	//it should return conflictTerm = log[prevLogIndex].Term,
	//and then search its log for the first index whose entry has term equal to conflictTerm.
	matchIndex := rf.logIndex(args.PrevLogIndex)
	if rf.log[matchIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = rf.log[matchIndex].Term
		reply.XIndex = args.PrevLogIndex
		index := args.PrevLogIndex
		for index > rf.commitIndex && rf.log[rf.logIndex(index)].Term == reply.XTerm {
			index--
		}
		//退出循环时index指向最后一个不相等的位置，所以+1
		index++
		reply.XIndex = index
		reply.XLen = rf.logLength()
		return
	}
	//3. If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it (§5.3)
	//The if here is crucial. If the follower has all the entries the leader sent, the follower MUST NOT truncate its log.
	//Any elements following the entries sent by the leader MUST be kept.
	//This is because we could be receiving an outdated AppendEntries RPC from the leader,
	//and truncating the log would mean “taking back” entries that we may have already told the leader that we have in our log.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i //日志的完整数组的索引
		if index < rf.lastLogIndex()+1 {   //follower可能包含了新的日志
			if rf.log[rf.logIndex(index)].Term != entry.Term { //说明这个新日志是follower没有的
				rf.log = rf.log[:rf.logIndex(index)] // 删除当前以及后续所有log
				rf.log = append(rf.log, entry)       // 把新log加入进来
			}
		} else if index == rf.lastLogIndex()+1 { //刚好是下一个
			//4. Append any new entries not already in the log
			DPrintf("Server %d append the log whose index is %d, and term is %d", rf.me, index, entry.Term)
			rf.log = append(rf.log, entry)
		}
	}
	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.cond.Broadcast()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) handleAppendEntries(peer int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	//DPrintf("Leader %d send a heartbest to Server %d\n", rf.me, peer)
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//Term confusion refers to servers getting confused by RPCs that come from old terms.
		//In general, this is not a problem when receiving an RPC,
		//since the rules in Figure 2 say exactly what you should do when you see an old term.
		//However, Figure 2 generally doesn’t discuss what you should do when you get old RPC replies.
		//From experience, we have found that by far the simplest thing to do is to first record the term in the reply
		//(it may be higher than your current term), and then to compare the current term with the term you sent in your original RPC.
		//If the two are different, drop the reply and return.
		//Only if the two terms are the same should you continue processing the reply.
		//There may be further optimizations you can do here with some clever protocol reasoning, but this approach seems to work well.
		//And not doing it leads down a long, winding path of blood, sweat, tears and despair.
		//简单来说就是要抛弃掉currentTerm != args.Term的回复
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			DPrintf("Leader %d find its term is too late", rf.me)
			rf.stateChanged(Follower)
			return
		}
		//A related, but not identical problem is that of assuming that your state has not changed between when you sent the RPC,
		//and when you received the reply. A good example of this is setting matchIndex = nextIndex - 1,
		//or matchIndex = len(log) when you receive a response to an RPC.
		//This is not safe, because both of those values could have been updated since when you sent the RPC.
		//Instead, the correct thing to do is update matchIndex to be prevLogIndex + len(entries[]) from the arguments you sent in the RPC originally.
		if reply.Success {
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
			rf.commitLogs()
		} else {
			if reply.XTerm == -1 {
				//  Case 3: follower's log is too short:
				//    nextIndex = XLen
				rf.nextIndex[peer] = reply.XLen
				return
			}
			//Upon receiving a conflict response, the leader should first search its log for conflictTerm.
			//If it finds an entry in its log with that term,
			//it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
			lastIndexOfXTerm := -1
			for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
				if rf.log[i].Term == reply.XTerm {
					lastIndexOfXTerm = rf.realIndex(i)
					break
				}
			}
			//If it does not find an entry with that term, it should set nextIndex = conflictIndex.
			//  Case 1: leader doesn't have XTerm:
			//    nextIndex = XIndex
			if lastIndexOfXTerm == -1 {
				rf.nextIndex[peer] = reply.XIndex
			} else {
				rf.nextIndex[peer] = lastIndexOfXTerm + 1
			}
		}
	}
}

func (rf *Raft) commitLogs() {
	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N (§5.3, §5.4).
	for N := rf.lastLogIndex(); N > rf.commitIndex && rf.log[rf.logIndex(N)].Term == rf.currentTerm; N-- {
		count := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.cond.Broadcast()
		}
	}
}
