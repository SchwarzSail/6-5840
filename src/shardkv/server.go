package shardkv

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"github.com/sasha-s/go-deadlock"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType        string
	ResultMsg     chan string
	ErrMsg        chan Err
	From          int
	Key           string
	Value         string
	ClientID      int64
	SequentID     int
	NewConfig     *shardctrler.Config
	MigrationArgs *ShardMigrationArgs
}

type RequestInfo struct {
	ClientID  int64
	SequentID int
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
	dead            int32
	persister       *raft.Persister
	storage         map[string]string
	clientTable     map[int64]int //client --> sequent
	requestTable    map[int]*RequestInfo
	duplicatedTable map[DuplicatedKey]string //record duplicated result
	lastApplied     int
	//for duplicated dection
	clientID  int64
	sequentID int

	mck *shardctrler.Clerk
	cfg *shardctrler.Config
	//shard
	shardToBeSentTable map[ShardInfo]ShardMigrationInfo
	// when server receive the shard data, but data's version is higher than the current config, we need to cache the data
	shardRequestCache map[ShardInfo]*ShardMigrationArgs
	shardTobeReceived map[int]int
	//config
	cond *sync.Cond
}

func (kv *ShardKV) preProcessRequest(key string, clientID int64, sequentID int, err *Err, value *string) bool {
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
				key := DuplicatedKey{
					Shard:    key2shard(key),
					ClientID: clientID,
				}
				*value = kv.duplicatedTable[key]
			}
			Debug(dInfo, "Find that the request whose ClientID is %d, and SequentID is %d is duplicated", clientID, sequentID)
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

func (kv *ShardKV) saveRequestInstance(index int, info *RequestInfo) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.requestTable[index] = info
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if ok := kv.preProcessRequest(args.Key, args.ClientID, args.SequentID, &reply.Err, &reply.Value); !ok {
		return
	}
	//建立对应的channel
	ch := make(chan string, 1)
	errCh := make(chan Err, 1)
	//封装日志发送给raft
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{
		Key:       args.Key,
		OpType:    "Get",
		From:      kv.me,
		ResultMsg: ch,
		ErrMsg:    errCh,
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
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
	}
	kv.saveRequestInstance(index, &info)
	//等待apply()
	select {
	case value := <-ch:
		reply.Err = OK
		reply.Value = value
		Debug(dInfo, "service get the success reply of Get")
	case err := <-errCh:
		Debug(dWarn, "PutAppend: The Err is %v", err)
		reply.Err = err
	case <-time.After(RPCTimeout):
		Debug(dWarn, "Server %d find that the Get RPC is timeout, and ClientID is %d, SequentID is %d", kv.me, args.ClientID, args.SequentID)
		reply.Err = ErrRPCTimeout
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if ok := kv.preProcessRequest(args.Key, args.ClientID, args.SequentID, &reply.Err, nil); !ok {
		return
	}
	//建立对应的channel
	ch := make(chan string, 1)
	errCh := make(chan Err, 1)
	//封装日志发送给raft
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{
		Key:       args.Key,
		Value:     args.Value,
		OpType:    args.Op,
		From:      kv.me,
		ResultMsg: ch,
		ErrMsg:    errCh,
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
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
	}
	kv.saveRequestInstance(index, &info)
	//等待apply()
	select {
	case <-ch:
		reply.Err = OK
		Debug(dInfo, "service get the success reply of PutAppend")
	case err := <-errCh:
		reply.Err = err
		Debug(dWarn, "Get: The Err is %v", err)
	case <-time.After(RPCTimeout):
		Debug(dWarn, "Server %d find that the PutAppend RPC is timeout, and ClientID is %d, SequentID is %d", kv.me, args.ClientID, args.SequentID)
		reply.Err = ErrRPCTimeout
	}
}

// Kill the tester calls Kill() when a ShardKV instance won't
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

// StartServer servers[] contains the ports of the servers in this group.
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
	labgob.Register(&RequestInfo{})
	labgob.Register(&ShardMigrationArgs{})
	labgob.Register(&ShardMigrationReply{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.cond = sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.storage = make(map[string]string)
	kv.clientTable = make(map[int64]int)
	kv.requestTable = make(map[int]*RequestInfo)
	kv.shardToBeSentTable = make(map[ShardInfo]ShardMigrationInfo)
	kv.shardRequestCache = make(map[ShardInfo]*ShardMigrationArgs)
	kv.duplicatedTable = make(map[DuplicatedKey]string)
	kv.shardTobeReceived = make(map[int]int)
	kv.clientID = nrand()
	cfg := kv.mck.Query(-1)
	kv.cfg = &cfg
	go kv.processApply()
	go kv.processMonitor()
	go kv.processSendingShards()
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
		//if msg.SnapshotValid {
		//	if msg.SnapshotIndex <= kv.lastApplied {
		//		Debug(dWarn, "the snapshot is duplicated or expired")
		//		continue
		//	}
		//	kv.readFromSnapshot(msg.Snapshot)
		//	kv.mu.Lock()
		//	kv.lastApplied = msg.SnapshotIndex
		//	kv.mu.Unlock()
		//}
		if msg.CommandValid {
			op := msg.Command.(Op)
			//Debug(dTrace, "Server %d start to apply the cmd whose index is %d",kv.me, msg.CommandIndex)
			kv.judgeInstance(op, msg.CommandIndex)
			if msg.CommandIndex <= kv.lastApplied {
				Debug(dWarn, "the apply is duplicated or expired")
				continue
			}
			switch op.OpType {
			case "UpdateConfig":
				//Debug(dInfo, "Server %d start to updateConfig", kv.me)
				kv.updateConfig(op)
			case "ReceiveShards":
				kv.migrateShards(op)
				if op.From == kv.me && op.ResultMsg != nil {
					op.ResultMsg <- ""
				}
			default:
				//before we execute the command, we need to check the key is whether the server is responsible for
				shard := key2shard(op.Key)
				gid, isNotReady := kv.shardTobeReceived[shard]
				if kv.cfg.Shards[shard] != kv.gid {
					if op.From == kv.me && op.ErrMsg != nil {
						op.ErrMsg <- ErrWrongGroup
					}
				} else if isNotReady {
					if op.From == kv.me && op.ErrMsg != nil {
						Debug(dInfo, "expect receive the shard from %d", gid)
						op.ErrMsg <- ErrShardNotReady
					}
				} else {
					//execute command
					res := kv.executeCommand(op, msg.CommandIndex)
					if op.From == kv.me && op.ResultMsg != nil {
						Debug(dTrace, "Server %d call the RPC to continue", kv.me)
						op.ResultMsg <- res
					}
					//if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					//	kv.persist(msg.CommandIndex)
					//}
				}

			}
		}
	}
}

func (kv *ShardKV) freeMemory() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for key, _ := range kv.requestTable {
		delete(kv.requestTable, key)
	}
}

func (kv *ShardKV) judgeInstance(op Op, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	info, ok := kv.requestTable[index]
	//if the request is invalid, we need to notify the corresponding client
	if ok && (info.ClientID != op.ClientID || info.SequentID != op.SequentID) {
		Debug(dTrace, "Server %d find that the request is invalid", kv.me)
		op.ErrMsg <- ErrWrongRequest
	}
	delete(kv.requestTable, index)
}

func (kv *ShardKV) executeCommand(op Op, index int) (res string) {
	preSequentID, ok := kv.clientTable[op.ClientID]
	key := DuplicatedKey{
		Shard:    key2shard(op.Key),
		ClientID: op.ClientID,
	}
	if ok && preSequentID == op.SequentID {
		res = kv.duplicatedTable[key]
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
	kv.duplicatedTable[key] = res
	Debug(dTrace, "Server %d apply the cmd whose clientID is %d, and sequentID is %d", kv.me, op.ClientID, op.SequentID)
	kv.lastApplied = index
	return
}
