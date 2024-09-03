package shardkv

import (
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"github.com/sasha-s/go-deadlock"
)



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	ResultChannel chan *config
	From int
	Key string
	Value string
	ClientID int64
	SequentID int
}

type RequestInfo struct {
	ClientID  int64
	SequentID int
	ErrChannel chan Err
	Result string
}

type ShardKV struct {
	mu           deadlock.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32
	persister       *raft.Persister
	storage         map[string]string
	clientTable     map[int64]int //client --> sequent
	requestTable    map[int]*RequestInfo
	duplicatedTable map[int64]string //record duplicated result

	lastApplied int
}


func (kv *ShardKV) preProcessRequest(clientID int64, sequentID int, err *Err, value *string) bool {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		*err = ErrWrongLeader
		return false
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if preSequentID, ok := kv.clientTable[clientID]; ok {
		if preSequentID == sequentID {
			*err = OK
			if value != nil {
				*value = kv.duplicatedTable[clientID]
			}
			Debug(dInfo, "Find that the request whose ClientID is %d, and SequentID is %d is duplicated", clientID,sequentID)
			return false
		} else if preSequentID > sequentID {
			*err = ErrExpireReq
			return false
		} else {
			return true
		}
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}



// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	go kv.processApply()
	return kv
}

// service 用于处理applyCh
func (kv *ShardKV) processApply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		//需要时刻检测当前是否为leader
		if msg.TermUpdated {
			kv.freeMemory()
			continue
		}
		if msg.SnapshotValid {
			if msg.SnapshotIndex <= kv.lastApplied {
				Debug(dWarn, "the snapshot is duplicated or expired")
				continue
			}
			kv.readFromSnapshot(msg.Snapshot)
			kv.mu.Lock()
			kv.lastApplied = msg.SnapshotIndex
			kv.mu.Unlock()
		} else if msg.CommandValid {
			//判断错误请求
			//Debug(dTrace, "Server %d start to apply the cmd whose index is %d",kv.me, msg.CommandIndex)
			op := msg.Command.(Op)
			kv.judgeInstance(op, msg.CommandIndex)
			if msg.CommandIndex <= kv.lastApplied {
				Debug(dWarn, "the apply is duplicated or expired")
				continue
			}
			//执行command
			res := kv.executeCommand(op, msg.CommandIndex)
			
			if op.From == kv.me && op.ResultChannel != nil {
				Debug(dTrace, "Server %d call the RPC to continue",kv.me)
				op.ResultChannel <- res
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.persist(msg.CommandIndex)
			}
		}
	}
}


func (kv *ShardKV) freeMemory() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for key, val := range kv.requestTable {
		val.ErrChannel <- ErrWrongLeader //通过channel告知RPC结束进程
		delete(kv.requestTable, key)
	}
}

func (kv *ShardKV) judgeInstance(op Op, index int) {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	info, ok := kv.requestTable[index]
	//保证请求实例和apply接收到的应该是一样的
	if ok && (info.ClientID != op.ClientID || info.SequentID != op.SequentID) {
		//通知rpc进程
		Debug(dTrace, "Server %d find that the request is invalid",kv.me)
		info.ErrChannel <- ErrWrongRequest
	}
	delete(kv.requestTable, index)
}

func (kv *ShardKV) executeCommand(op Op, index int) (res string) {
	preSequentID, ok := kv.clientTable[op.ClientID]
	if ok && preSequentID == op.SequentID {
		res = kv.duplicatedTable[op.ClientID]
		Debug(dTrace, "Server %d find the cmd is duplicated", kv.me)
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.clientTable[op.ClientID] = op.SequentID
	switch op.OpType {
	case "Get":
		res = kv.storage[op.Key]
	case "Put":
		kv.storage[op.Key] = op.Value
		res = op.Value
	case "Append":
		val := kv.storage[op.Key]
		kv.storage[op.Key] = val + op.Value
		res = kv.storage[op.Key]
	}
	kv.duplicatedTable[op.ClientID] = res
	Debug(dTrace, "Server %d apply the cmd whose clientID is %d, and sequentID is %d", kv.me, op.ClientID, op.SequentID)
	kv.lastApplied = index
	return
}
