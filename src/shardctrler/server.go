package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead            int32
	configs         []Config // indexed by config num
	requestTable    map[int]*RequestInfo
	duplicatedTable map[int64]LastReply
	lastApplied     int
}

type Op struct {
	// Your data here.
	OpType        string
	ResultChannel chan Config
	From          int
	Args          interface{}
}

func (op *Op) GetClientID() int64 {
	switch op.OpType {
	case "Join":
		return op.Args.(*JoinArgs).ClientID
	case "Leave":
		return op.Args.(*LeaveArgs).ClientID
	case "Move":
		return op.Args.(*MoveArgs).ClientID
	case "Query":
		return op.Args.(*QueryArgs).ClientID
	}
	return 0
}

func (op *Op) GetSequentID() int {
	switch op.OpType {
	case "Join":
		return op.Args.(*JoinArgs).SequentID
	case "Leave":
		return op.Args.(*LeaveArgs).SequentID
	case "Move":
		return op.Args.(*MoveArgs).SequentID
	case "Query":
		return op.Args.(*QueryArgs).SequentID
	}
	return 0
}

type RequestInfo struct {
	ClientID  int64
	SequentID int
	Result    chan Err
}

// 对每个rpc请求进行预处理
func (sc *ShardCtrler) preProcessRequest(clientID int64, sequentID int, err *Err, value *Config) bool {
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		*err = ErrWrongLeader
		return false
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastReply, ok := sc.duplicatedTable[clientID]
	if !ok {
		return true
	}
	switch {
	case lastReply.SequentID == sequentID:
		*err = OK
		if value != nil {
			*value = sc.duplicatedTable[clientID].Value
		}
		return true
	case lastReply.SequentID > sequentID:
		*err = ErrExpireReq
		return false
	default:
		return true
	}
}

func (sc *ShardCtrler) saveRequestInstance(index int, req *RequestInfo) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.requestTable[index] = req
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if ok := sc.preProcessRequest(args.ClientID, args.SequentID, &reply.Err, nil); !ok {
		if reply.Err == ErrWrongLeader {
			reply.WrongLeader = true
		}
		Debug(dTrace, "Join operation is invalid")
		return
	}
	ch := make(chan Config)
	errCh := make(chan Err)
	index, _, isLeader := sc.rf.Start(Op{
		OpType:        "Join",
		ResultChannel: ch,
		From:          sc.me,
		Args:          args,
	})
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
	//save the request instance
	requestInfo := &RequestInfo{
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
		Result:    errCh,
	}
	sc.saveRequestInstance(index, requestInfo)

	select {
	case <-ch:
		reply.Err = OK
		return
	case err := <-errCh:
		reply.WrongLeader = true
		reply.Err = err
	case <-time.After(RPCTimeout):
		reply.Err = ErrRPCTimeout
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if ok := sc.preProcessRequest(args.ClientID, args.SequentID, &reply.Err, nil); !ok {
		if reply.Err == ErrWrongLeader {
			reply.WrongLeader = true
		}
		Debug(dTrace, "Leave operation is invalid")
		return
	}
	ch := make(chan Config)
	errCh := make(chan Err)
	index, _, isLeader := sc.rf.Start(Op{
		OpType:        "Leave",
		ResultChannel: ch,
		From:          sc.me,
		Args:          args,
	})
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
	//save the request instance
	requestInfo := &RequestInfo{
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
		Result:    errCh,
	}
	sc.saveRequestInstance(index, requestInfo)

	select {
	case <-ch:
		reply.Err = OK
		return
	case err := <-errCh:
		reply.WrongLeader = true
		reply.Err = err
	case <-time.After(RPCTimeout):
		reply.Err = ErrRPCTimeout
	}

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if ok := sc.preProcessRequest(args.ClientID, args.SequentID, &reply.Err, nil); !ok {
		if reply.Err == ErrWrongLeader {
			reply.WrongLeader = true
		}
		Debug(dTrace, "Move operation is invalid")
		return
	}
	ch := make(chan Config)
	errCh := make(chan Err)
	index, _, isLeader := sc.rf.Start(Op{
		OpType:        "Move",
		ResultChannel: ch,
		From:          sc.me,
		Args:          args,
	})
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
	//save the request instance
	requestInfo := &RequestInfo{
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
		Result:    errCh,
	}
	sc.saveRequestInstance(index, requestInfo)

	select {
	case <-ch:
		reply.Err = OK
		return
	case err := <-errCh:
		reply.WrongLeader = true
		reply.Err = err
	case <-time.After(RPCTimeout):
		reply.Err = ErrRPCTimeout
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	//if ok := sc.preProcessRequest(args.ClientID, args.SequentID, &reply.Err, &reply.Config); !ok {
	//	reply.WrongLeader = true
	//	Debug(dTrace, "Query operation is invalid")
	//	return
	//}
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan Config)
	errCh := make(chan Err)
	index, _, isLeader := sc.rf.Start(Op{
		OpType:        "Query",
		ResultChannel: ch,
		From:          sc.me,
		Args:          args,
	})
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
	//save the request instance
	requestInfo := &RequestInfo{
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
		Result:    errCh,
	}
	sc.saveRequestInstance(index, requestInfo)

	select {
	case value := <-ch:
		reply.Config = value
		reply.Err = OK
		return
	case err := <-errCh:
		reply.WrongLeader = true
		reply.Err = err
	case <-time.After(RPCTimeout):
		reply.Err = ErrRPCTimeout
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(&QueryArgs{})
	labgob.Register(&JoinArgs{})
	labgob.Register(&LeaveArgs{})
	labgob.Register(&MoveArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.requestTable = make(map[int]*RequestInfo)
	sc.duplicatedTable = make(map[int64]LastReply)
	sc.lastApplied = 0

	go sc.apply()
	return sc
}

func (sc *ShardCtrler) apply() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.TermUpdated {
			sc.freeMemory()
			continue
		}
		if msg.CommandValid {
			if msg.CommandIndex <= sc.lastApplied {
				Debug(dWarn, "The msg is duplicated or expired")
				continue
			}
			op := msg.Command.(Op)
			sc.judgeInstance(op, msg.CommandIndex)
			res := sc.executeCommand(op)
			if op.From == sc.me && op.ResultChannel != nil {
				Debug(dTrace, "Server %d call the RPC to continue", sc.me)
				op.ResultChannel <- res
			}
		}
	}
}

func (sc *ShardCtrler) freeMemory() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for key, val := range sc.requestTable {
		val.Result <- ErrWrongLeader //通过channel告知RPC结束进程
		delete(sc.requestTable, key)
	}
}

func (sc *ShardCtrler) judgeInstance(op Op, index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	info, ok := sc.requestTable[index]
	//保证请求实例和apply接收到的应该是一样的
	if ok && (info.ClientID != op.GetClientID() || info.SequentID != op.GetSequentID()) {
		//通知rpc进程
		Debug(dTrace, "Server %d find that the request is invalid", sc.me)
		info.Result <- ErrWrongRequest
	}
	delete(sc.requestTable, index)
}

func (sc *ShardCtrler) executeCommand(op Op) (res Config) {
	//judge the duplicated command
	lastReply, ok := sc.duplicatedTable[op.GetClientID()]
	if ok && lastReply.SequentID == op.GetSequentID() {
		Debug(dInfo, "Server %d find that the request is duplicated whose ClientID is %d, SequentID is %d", sc.me, op.GetClientID(), op.GetSequentID())
		res = sc.duplicatedTable[op.GetClientID()].Value
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	switch op.OpType {
	case "Join":
		sc.join(op.Args.(*JoinArgs))
	case "Leave":
		sc.leave(op.Args.(*LeaveArgs))
	case "Move":
		sc.move(op.Args.(*MoveArgs))
	case "Query":
		res = *sc.query(op.Args.(*QueryArgs))
	}
	sc.duplicatedTable[op.GetClientID()] = LastReply{
		SequentID: op.GetSequentID(),
		Value:     res,
	}
	return
}

func idealLoad(totalShards int, totalGroups int) (int, int) {
	return totalShards / totalGroups, totalShards % totalGroups
}

func (sc *ShardCtrler) loadBalance(cfg *Config) {
	gidToShards := make(map[int]int)
	Debug(dTrace, "cfg.Shards is %v\n", cfg.Shards)
	Debug(dTrace, "cfg.Groups is %v\n", cfg.Groups)
	//calculate the number of shards that each origin gid is responsible for
	for _, value := range cfg.Shards {
		if value > 0 {
			gidToShards[value]++
		}
	}
	Debug(dTrace, "The origin gidToshards is %v\n", gidToShards)

	//to grantee all request has the same result
	gids := []int{}
	for gid := range cfg.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	h := NewHeap()
	for _, gid := range gids {
		if _, ok := gidToShards[gid]; !ok {
			gidToShards[gid] = 0
		}
		h.push(&GidShardsMapping{Gid: gid, ShardCount: gidToShards[gid]})
	}
	avg, remain := idealLoad(NShards, len(gids))
	//record the gid that has remainder
	duplicatedMap := make(map[int]bool)

	//record the shard that have no gid to be responsible for
	shardAssignMapping := make([]int, 0, NShards)
	for i := 0; i < NShards; i++ {
		gid := cfg.Shards[i]
		//it means that the shard has no gid to be responsible for
		if gid == 0 {
			shardAssignMapping = append(shardAssignMapping, i)
		} else if gidToShards[gid] > avg {
			if _, exist := duplicatedMap[gid]; exist && gidToShards[gid] == avg+1 {
				continue
			}
			//the length of duplicatedMap must be less than remain
			if gidToShards[gid] == avg+1 && len(duplicatedMap) < remain {
				duplicatedMap[gid] = true
				continue
			}
			//the gid have more shards than the average, we need to move the shard to other gids
			shardAssignMapping = append(shardAssignMapping, i)
			cfg.Shards[i] = 0
			gidToShards[gid]--
			h.update(gid, gidToShards[gid])
		}
	}

	//assign the shards
	for len(shardAssignMapping) > 0 {
		minElement := h.pop()
		shard := shardAssignMapping[0]
		shardAssignMapping = shardAssignMapping[1:]
		cfg.Shards[shard] = minElement.Gid
		gidToShards[minElement.Gid]++
		minElement.ShardCount++
		h.push(minElement)
	}
}
func (sc *ShardCtrler) join(args *JoinArgs) {
	lastConfig := sc.configs[len(sc.configs)-1].clone()
	lastConfig.Num++

	for gid, servers := range args.Servers {
		lastConfig.Groups[gid] = servers
	}
	sc.loadBalance(lastConfig)
	Debug(dInfo, "Join Operation, ClientID: %v, SequentID: %v, shards: %v\n", args.ClientID, args.SequentID, lastConfig.Shards)
	sc.configs = append(sc.configs, *lastConfig)
}

func (sc *ShardCtrler) leave(args *LeaveArgs) {
	lastConfig := sc.configs[len(sc.configs)-1].clone()
	lastConfig.Num++
	for _, gid := range args.GIDs {
		for i := 0; i < NShards; i++ {
			if lastConfig.Shards[i] == gid {
				lastConfig.Shards[i] = 0
			}
		}
		delete(lastConfig.Groups, gid)
	}
	if len(lastConfig.Groups) != 0 {
		sc.loadBalance(lastConfig)
	}
	Debug(dInfo, "Leave Operation, ClientID: %v, SequentID: %v, shards: %v\n", args.ClientID, args.SequentID, lastConfig.Shards)
	sc.configs = append(sc.configs, *lastConfig)
}

func (sc *ShardCtrler) move(args *MoveArgs) {
	lastConfig := sc.configs[len(sc.configs)-1].clone()
	lastConfig.Num++
	lastConfig.Shards[args.Shard] = args.GID
	sc.loadBalance(lastConfig)
	Debug(dInfo, "Move Operation, ClientID: %v, SequentID: %v, shards: %v\n", args.ClientID, args.SequentID, lastConfig.Shards)
	sc.configs = append(sc.configs, *lastConfig)
}

func (sc *ShardCtrler) query(args *QueryArgs) *Config {
	if args.Num < 0 || args.Num >= len(sc.configs) {
		return &sc.configs[len(sc.configs)-1]
	}
	return &sc.configs[args.Num]
}
