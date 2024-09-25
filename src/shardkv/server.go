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
	OpType          string
	From            int
	Key             string
	Value           string
	ClientID        int64
	SequentID       int
	ShardID         int
	Version         int
	NewConfig       shardctrler.Config
	Data            map[string]string
	DuplicatedTable map[int64]LastReply
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
	storage         map[int]map[string]string
	duplicatedTable map[int64]LastReply //record duplicated result
	requestTable    map[int]*RequestInfo
	lastApplied     int
	//for duplicated dection
	clientID  int64
	sequentID int

	mck    *shardctrler.Clerk
	config atomic.Pointer[shardctrler.Config]
	//shard
	shards []State //record the shard state that current server is responsible for
	//config
	cond *sync.Cond
}

func (kv *ShardKV) preProcessRequest(clientID int64, sequentID int, err *Err, value *string) bool {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		*err = ErrWrongLeader
		return false
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastReply, ok := kv.duplicatedTable[clientID]
	if !ok {
		return true
	}
	switch {
	case lastReply.SequentID == sequentID:
		*err = OK
		if value != nil {
			*value = lastReply.Value
		}
		return false
	case lastReply.SequentID > sequentID:
		*err = ErrExpireReq
		Debug(dTrace, "[%d] [%d] server %d find that the request %d %d is expired", kv.gid, kv.config.Load().Num, kv.me, clientID, sequentID)
		return false
	default:
		return true
	}
}

func (kv *ShardKV) saveRequestInstance(index int, info *RequestInfo) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.requestTable[index] = info
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
		Key:       args.Key,
		ShardID:   key2shard(args.Key),
		OpType:    "Get",
		From:      kv.me,
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
		ErrMsg:    errCh,
		Result:    ch,
	}
	kv.saveRequestInstance(index, &info)
	//等待apply()
	defer kv.freeMemory(index)

	select {
	case value := <-ch:
		reply.Err = OK
		reply.Value = value
		//kv.mu.Lock()
		//Debug(dTrace, "[%d] [%d] Leader %d success to get the reply of Get [%d], key %v, res %v", kv.gid, kv.config.Load().Num, kv.me, key2shard(args.Key), args.Key, value)
		//kv.mu.Unlock()
	case err := <-errCh:
		Debug(dWarn, "Get: [%d] [%d] server %d, The Err is %v", kv.gid, kv.config.Load().Num, kv.me, err)
		reply.Err = err
	case <-time.After(RPCTimeout):
		Debug(dWarn, "Get: [%d] [%d] Server %d find that the Get RPC is timeout, and ClientID is %d, SequentID is %d", kv.gid, kv.config.Load().Num, kv.me, args.ClientID, args.SequentID)
		reply.Err = ErrRPCTimeout
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if ok := kv.preProcessRequest(args.ClientID, args.SequentID, &reply.Err, nil); !ok {
		return
	}
	//建立对应的channel
	ch := make(chan string, 1)
	errCh := make(chan Err, 1)
	//封装日志发送给raft
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{
		ShardID:   key2shard(args.Key),
		Key:       args.Key,
		Value:     args.Value,
		OpType:    args.Op,
		From:      kv.me,
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
		ErrMsg:    errCh,
		Result:    ch,
	}
	kv.saveRequestInstance(index, &info)
	//等待apply()
	defer kv.freeMemory(index)
	select {
	case <-ch:
		reply.Err = OK
		//for debug
		//kv.mu.Lock()
		//Debug(dTrace, "[%d] [%d] Leader %d success to get the reply of PutAppend [%d], key %v, res %v", kv.gid, kv.config.Load().Num, kv.me, key2shard(args.Key), args.Key, res)
		//kv.mu.Unlock()
	case err := <-errCh:
		reply.Err = err
		Debug(dWarn, "PutAppend: [%d] [%d] server %d, The Err is %v", kv.gid, kv.config.Load().Num, kv.me, err)
	case <-time.After(RPCTimeout):
		Debug(dWarn, "PutAppend: [%d] [%d] Server %d find that the Get RPC is timeout, and ClientID is %d, SequentID is %d", kv.gid, kv.config.Load().Num, kv.me, args.ClientID, args.SequentID)
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.cond = sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.storage = make(map[int]map[string]string)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.storage[i] = make(map[string]string)
	}
	kv.duplicatedTable = make(map[int64]LastReply)
	kv.requestTable = make(map[int]*RequestInfo)
	kv.shards = make([]State, shardctrler.NShards)
	kv.clientID = nrand()
	cfg := kv.mck.Query(0)
	kv.config.Store(&cfg)
	kv.readFromSnapshot(kv.persister.ReadSnapshot())
	go kv.processApply()
	go kv.processMonitor()
	go kv.migratingDaemon()
	return kv
}

// service 用于处理applyCh
func (kv *ShardKV) processApply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		//check that the term isn't changed
		//in my views, term meas the status of consensus, if term changed, the consensus is invalid
		if msg.TermUpdated {
			kv.freeAllRequests()
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
		}
		if msg.CommandValid {
			op := msg.Command.(Op)
			if msg.CommandIndex <= kv.lastApplied {
				Debug(dWarn, "the apply is duplicated or expired")
				continue
			}

			kv.judgeInstance(op, msg.CommandIndex)
			errCh, result := kv.getChannels(msg.CommandIndex)
			switch op.OpType {
			case "Nothing":
			case "UpdateConfig":
				kv.handleUpdateConfig(op)
			case "UpdateMigrateState":
				kv.handleUpdateMigrateState(op, errCh)
				if op.From == kv.me && result != nil {
					result <- ""
				}
			case "Receive":
				kv.handleReceive(op, errCh)
				if op.From == kv.me && result != nil {
					result <- ""
				}
			default:
				//before we execute the command, we need to check the key is whether the server is responsible for
				isReady := kv.shards[op.ShardID] == Ready
				if kv.config.Load().Shards[op.ShardID] != kv.gid {
					if op.From == kv.me && errCh != nil {
						errCh <- ErrWrongGroup
					}
				} else if !isReady {
					if op.From == kv.me && errCh != nil {
						errCh <- ErrShardNotReady
					}
				} else {
					//execute command
					res := kv.executeCommand(op)
					if op.From == kv.me && result != nil {
						//xDebug(dTrace, "[%d] [%d] Server %d call the RPC to continue", kv.gid, kv.config.Load().Num, kv.me)
						result <- res
					}
				}
			}
			kv.mu.Lock()
			kv.lastApplied = msg.CommandIndex
			kv.mu.Unlock()
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.persist(kv.lastApplied)
			}
		}

	}
}

func (kv *ShardKV) freeAllRequests() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for key, _ := range kv.requestTable {
		delete(kv.requestTable, key)
	}
}

func (kv *ShardKV) freeMemory(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.requestTable, index)
}

func (kv *ShardKV) judgeInstance(op Op, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	info, ok := kv.requestTable[index]
	//if the request is invalid, we need to notify the corresponding client
	if ok && (info.ClientID != op.ClientID || info.SequentID != op.SequentID) {
		Debug(dTrace, "Server %d find that the request is invalid", kv.me)
		if info.ErrMsg != nil {
			info.ErrMsg <- ErrWrongRequest
		}
	}
	return
}

func (kv *ShardKV) getChannels(index int) (errCh chan Err, result chan string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	info, ok := kv.requestTable[index]
	if !ok {
		return nil, nil
	}
	return info.ErrMsg, info.Result
}

func (kv *ShardKV) executeCommand(op Op) (res string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastReply, ok := kv.duplicatedTable[op.ClientID]
	shard := key2shard(op.Key)
	if ok && lastReply.SequentID == op.SequentID {
		res = kv.duplicatedTable[op.ClientID].Value
		Debug(dTrace, "[%d] [%d] Server %d find the cmd is duplicated", kv.gid, kv.config.Load().Num, kv.me)
		return
	}
	switch op.OpType {
	case "Get":
		res = kv.storage[shard][op.Key]
	case "Put":
		kv.storage[shard][op.Key] = op.Value
		res = op.Value
	case "Append":
		val := kv.storage[shard][op.Key]
		kv.storage[shard][op.Key] = val + op.Value
		res = kv.storage[shard][op.Key]
	}
	kv.duplicatedTable[op.ClientID] = LastReply{
		SequentID: op.SequentID,
		Value:     res,
	}
	//Debug(dTrace, "[%d] [%d] Server %d apply the cmd whose clientID is %d, and sequentID is %d", kv.gid, kv.config.Load().Num, kv.me, op.ClientID, op.SequentID)
	return
}
