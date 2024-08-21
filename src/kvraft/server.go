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
	Key           string
	Value         string
	Op            string
	From          int
	ResultChannel chan string
	ClientID      int64
	SequentID     int
}

type RequestInfo struct {
	ClientID  int64
	SequentID int
	Result    chan Err
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	persister       *raft.Persister
	storage         map[string]string
	clientTable     map[int64]int //client --> sequent
	requestTable    map[int]*RequestInfo
	duplicatedTable map[int64]string //保存重复的请求结果
	//当server收到有效的request时，会将request实例保存到本地
	lastApplied int
}

// 对每个rpc请求进行预处理
func (kv *KVServer) preProcessRequest(clientID int64, sequentID int, err *Err, value *string) bool {
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
			Debug(dInfo, "Find that the request is duplicated")
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

func (kv *KVServer) saveRequestInstance(index int, info *RequestInfo) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.requestTable[index] = info
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if ok := kv.preProcessRequest(args.ClientID, args.SequentID, &reply.Err, &reply.Value); !ok {
		return
	}
	//建立对应的channel
	ch := make(chan string, 1)
	errCh := make(chan Err, 1)
	//封装日志发送给raft
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{
		Key:           args.Key,
		Op:            "Get",
		From:          kv.me,
		ResultChannel: ch,
		ClientID:      args.ClientID,
		SequentID:     args.SequentID,
	})
	kv.mu.Unlock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//保存对应实例
	info := RequestInfo{
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
		Result:    errCh,
	}
	kv.saveRequestInstance(index, &info)
	//等待apply()
	select {
	case value := <-ch:
		reply.Err = OK
		reply.Value = value
		Debug(dInfo, "service get the success reply of Get")
	case err := <-errCh:
		Debug(dWarn, "---------------Leader changed------------------------")
		reply.Err = err
	case <-time.After(RPCTimeout):
		reply.Err = ErrRPCTimeout
	}
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if ok := kv.preProcessRequest(args.ClientID, args.SequentID, &reply.Err, nil); !ok {
		return
	}
	//建立对应的channel
	ch := make(chan string, 1)
	errCh := make(chan Err, 1)
	//封装日志发送给raft
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{
		Key:           args.Key,
		Value:         args.Value,
		Op:            args.Op,
		From:          kv.me,
		ResultChannel: ch,
		ClientID:      args.ClientID,
		SequentID:     args.SequentID,
	})
	kv.mu.Unlock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//保存对应实例
	info := RequestInfo{
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
		Result:    errCh,
	}
	kv.saveRequestInstance(index, &info)
	//等待apply()
	select {
	case <-ch:
		reply.Err = OK
		Debug(dInfo, "service get the success reply of PutAppend")
	case err := <-errCh:
		reply.Err = err
		Debug(dWarn, "---------------Leader changed------------------------")
	case <-time.After(RPCTimeout):
		reply.Err = ErrRPCTimeout
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
	kv.requestTable = make(map[int]*RequestInfo)
	kv.duplicatedTable = make(map[int64]string)
	kv.lastApplied = 0
	kv.readFromSnapshot(kv.persister.ReadSnapshot())
	go kv.processApply()
	return kv
}

// service 用于处理applyCh
func (kv *KVServer) processApply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		//需要时刻检测当前是否为leader
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			kv.freeMemory()
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
			Debug(dTrace, "Server %d start to apply the cmd whose index is %d",kv.me, msg.CommandIndex)
			op := msg.Command.(Op)
			kv.judgeInstance(op, msg.CommandIndex)
			if msg.CommandIndex <= kv.lastApplied {
				Debug(dWarn, "the apply is duplicated or expired")
				continue
			}
			//执行command
			res := kv.executeCommand(op, msg.CommandIndex)
			//唤醒对应进程，再次判断自己是不是leader
			_, isLeader := kv.rf.GetState()
			if isLeader && op.From == kv.me && op.ResultChannel != nil {
				op.ResultChannel <- res
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.persist(msg.CommandIndex)
			}
		}
	}
}

func (kv *KVServer) freeMemory() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for key, val := range kv.requestTable {
		Debug(dWarn, "Server %d send the close channel to RPC---", kv.me)
		val.Result <- ErrWrongLeader //通过channel告知RPC结束进程
		delete(kv.requestTable, key)
	}
}

func (kv *KVServer) judgeInstance(op Op, index int) {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	info, ok := kv.requestTable[index]
	//保证请求实例和apply接收到的应该是一样的
	if ok && (info.ClientID != op.ClientID || info.SequentID != op.SequentID) {
		//通知rpc进程
		info.Result <- ErrWrongRequest
	}
	delete(kv.requestTable, index)
}

func (kv *KVServer) executeCommand(op Op, index int) (res string) {
	preSequentID, ok := kv.clientTable[op.ClientID]
	if ok && preSequentID == op.SequentID {
		res = kv.duplicatedTable[op.ClientID]
		Debug(dTrace, "Server find the cmd is duplicated")
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.clientTable[op.ClientID] = op.SequentID
	switch op.Op {
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
