package kvraft

import (
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"github.com/sasha-s/go-deadlock"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string
	From      int
	ClientID  int64
	SequentID int
}

type RequestInfo struct {
	ClientID  int64
	SequentID int
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	persister    *raft.Persister
	storage      map[string]string
	clientTable  map[int64]int //client --> sequent
	lastApplied  int
	channelTable map[int]chan RequestInfo //index --> requestID,用于service应用了日志后通知对应的RPC进程
	isLeader     bool
}

// 对每个rpc请求进行预处理
func (kv *KVServer) preProcessRequest(clientID int64, sequentID int, err *Err) bool {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		kv.mu.Lock()
		kv.isLeader = false
		kv.mu.Unlock()
		*err = ErrWrongLeader
		return false
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if seq, ok := kv.clientTable[clientID]; ok {
		if seq == sequentID {
			*err = ErrDuplicateReq
			Debug(dInfo, "Find that the request is duplicated")
			return false
		} else if seq > sequentID {
			*err = ErrExpireReq
			return false
		} else {
			return true
		}
	}
	return true
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if ok := kv.preProcessRequest(args.ClientID, args.SequentID, &reply.Err); !ok {
		if reply.Err == ErrDuplicateReq {
			kv.mu.Lock()
			reply.Value = kv.storage[args.Key]
			kv.mu.Unlock()
		}
		return
	}
	//封装日志发送给raft
	index, term, isLeader := kv.rf.Start(Op{
		Key:       args.Key,
		Op:        "Get",
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.isLeader = true
	kv.mu.Unlock()
	Debug(dInfo, "Get: application send the command whose index is %d, and term is %d", index, term)
	//建立对应的channel
	ch := kv.createChannel(index)
	defer kv.freeMemory(index)

	//Debug(dTrace, "service make the channel whose ClientID is %d , SequentID is %d, and index is %d", args.ClientID, args.SequentID, index)

	//等待apply()
	select {
	case msg := <-ch:
		if msg.ClientID == args.ClientID && msg.SequentID == args.SequentID {
			kv.mu.Lock()
			if val, ok := kv.storage[args.Key]; ok {
				reply.Value = val
			} else {
				reply.Value = ""
			}
			kv.mu.Unlock()
			reply.Err = OK
			Debug(dInfo, "service get the success reply of Get")
			return
		}
	case <-time.After(RPCTimeout):
		reply.Err = ErrRPCTimeout
		return
	}
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if ok := kv.preProcessRequest(args.ClientID, args.SequentID, &reply.Err); !ok {
		return
	}
	//封装日志发送给raft
	index, term, isLeader := kv.rf.Start(Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.isLeader = true
	kv.mu.Unlock()
	Debug(dInfo, "PutAppend: application send the command whose index is %d, and term is %d", index, term)
	//建立对应的channel
	ch := kv.createChannel(index)
	defer kv.freeMemory(index)
	//Debug(dTrace, "service make the channel whose ClientID is %d , SequentID is %d, and index is %d", args.ClientID, args.SequentID, index)

	//等待apply()
	select {
	case msg := <-ch:
		if msg.ClientID == args.ClientID && msg.SequentID == args.SequentID {
			Debug(dInfo, "service get the success reply of PutAppend")
			reply.Err = OK
			return
		}
	case <-time.After(RPCTimeout):
		reply.Err = ErrRPCTimeout
		return
	}

}
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.clientTable = make(map[int64]int)
	kv.channelTable = make(map[int]chan RequestInfo)
	kv.lastApplied = 0
	kv.readFromSnapshot(kv.persister.ReadSnapshot())
	go kv.processApply()
	return kv
}

// service 用于处理applyCh
func (kv *KVServer) processApply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.SnapshotValid {
			kv.mu.Lock()
			if msg.SnapshotIndex <= kv.lastApplied { //重复或者是过期的快照请求
				kv.mu.Unlock()
				Debug(dInfo, "duplicated request of snapshot")
				return
			}
			kv.readFromSnapshot(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
			kv.mu.Unlock()
		} else if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied { //重复或者是过期的apply请求
				kv.mu.Unlock()
				Debug(dInfo, "duplicated request of apply")
				return
			}
			op := msg.Command.(Op)
			if preSequentID, ok := kv.clientTable[op.ClientID]; !ok || preSequentID != op.SequentID {
				//记录本次rpc的请求
				kv.clientTable[op.ClientID] = op.SequentID
				switch op.Op {
				case "Put":
					kv.storage[op.Key] = op.Value
				case "Append":
					if value, ok := kv.storage[op.Key]; ok {
						kv.storage[op.Key] = value + op.Value
					} else {
						kv.storage[op.Key] = op.Value
					}
				}
				kv.mu.Unlock()
				//将数据写入channel告知RPC
				//只有leader才能收到这个由channel发送的消息
				kv.mu.Lock()
				ok := kv.isLeader
				kv.mu.Unlock()
				if ok {
					ch := kv.createChannel(msg.CommandIndex)
					Debug(dTrace, "service: the channel is %v, and index is %d", ch, msg.CommandIndex)
					ch <- RequestInfo{
						ClientID:  op.ClientID,
						SequentID: op.SequentID,
					}
					Debug(dInfo, "----------service send the reply to channel")

				}
				kv.mu.Lock()
				kv.lastApplied = msg.CommandIndex
				kv.mu.Unlock()
				kv.persist()
			} else {
				kv.mu.Unlock()
			}
		}

	}
}

// 根据日志的index创建对应的channel
func (kv *KVServer) createChannel(index int) chan RequestInfo {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.channelTable[index]
	if !ok {
		kv.channelTable[index] = make(chan RequestInfo, 1)
		ch = kv.channelTable[index]
	}
	return ch
}

func (kv *KVServer) freeMemory(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.channelTable, index)
}
