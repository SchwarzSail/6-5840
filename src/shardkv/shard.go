package shardkv

import (
	"fmt"
	"time"
)

func DeepCopyStorageMap(src map[string]string) map[string]string {
	fmt.Println(src)
	dst := make(map[string]string)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func DeepCopyDuplicatedTableMap(src map[int64]string) map[int64]string {
	dst := make(map[int64]string)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// ------------------------------------Migration----------------------------
// monitor the shards' migration
func (kv *ShardKV) migratingDaemon() {
	for !kv.killed() {

		//only leader can monitor it
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		//collect shards need to be migrated
		shardToBeMigrated := make([]int, 0)
		for shard, state := range kv.shards {
			if state == WaitingMigrated {
				shardToBeMigrated = append(shardToBeMigrated, shard)
			}
		}
		if len(shardToBeMigrated) != 0 {
			Debug(dInfo, "migratingDaemon: [%d] [%d] Leader %d is migrating shards %v", kv.gid, kv.config.Num, kv.me, shardToBeMigrated)
			kv.migrating(shardToBeMigrated)
		}
		kv.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
	}
}

// call RPC
func (kv *ShardKV) migrating(shards []int) {
	for _, shard := range shards {
		//prepare args
		args := &MigrationArgs{
			ShardID:         shard,
			Version:         kv.config.Num,
			Data:            make(map[string]string),
			DuplicatedTable: make(map[int64]string),
			ClientID:        kv.clientID,
			SequentID:       kv.sequentID,
		}
		kv.sequentID++
		args.Data = DeepCopyStorageMap(kv.storage[shard])
		args.DuplicatedTable = DeepCopyDuplicatedTableMap(kv.duplicatedTable[shard])
		//Call RPCs
		Debug(dTrace, "migrating: [%d] [%d] Leader %d: the config  Group is %v", kv.gid, kv.config.Num, kv.me, kv.config.Groups[kv.config.Shards[shard]])
		for _, server := range kv.config.Groups[kv.config.Shards[shard]] {
			go kv.handleMigrating(server, args, shard)
		}

	}
	time.Sleep(200 * time.Millisecond)
}

func (kv *ShardKV) handleMigrating(server string, args *MigrationArgs, shard int) {
	var reply MigrationReply
	srv := kv.make_end(server)
	ok := srv.Call("ShardKV.MigrateShards", args, &reply)
	if ok && reply.Success {
		Debug(dInfo, "handleMigrating: [%d] [%d] Leader %d migrate shard %d to %s success", kv.gid, kv.config.Num, kv.me, shard, server)
		kv.mu.Lock()
		//make the consensus
		ch := make(chan string)
		ErrCh := make(chan Err)
		index, _, isLeader := kv.rf.Start(Op{
			OpType:    "UpdateMigrateState",
			ClientID:  kv.clientID,
			SequentID: kv.sequentID,
			ResultMsg: ch,
			ShardID:   shard,
			Version:   args.Version,
			ErrMsg:    ErrCh,
		})
		if !isLeader {
			kv.mu.Unlock()
			return
		}
		kv.requestTable[index] = &RequestInfo{
			ClientID:  kv.clientID,
			SequentID: kv.sequentID,
		}
		kv.sequentID++
		kv.mu.Unlock()
		select {
		case <-ch:
			Debug(dInfo, "handleMigrating: [%d] [%d] Leader %d success make the consensus", kv.gid, kv.config.Num, kv.me)
		case err := <-ErrCh:
			Debug(dInfo, "handleMigrating: [%d] [%d] Leader %d failed make the consensus %v", kv.gid, kv.config.Num, kv.me, err)
		case <-time.After(RPCTimeout):
			Debug(dInfo, "handleMigrating: [%d] [%d] Leader %d timeout make the consensus", kv.gid, kv.config.Num, kv.me)
		}
	} else {
		Debug(dInfo, "handleMigrating: [%d] [%d] Leader %d migrate shard %d to %s failed", kv.gid, kv.config.Num, kv.me, shard, server)
	}
}

// MigrateShards other send shards to current server
func (kv *ShardKV) MigrateShards(args *MigrationArgs, reply *MigrationReply) {
	//check the version
	kv.mu.Lock()
	if kv.config.Num < args.Version {
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d has  smaller version %v", kv.gid, kv.config.Num, kv.me, args.Version)
		reply.Success = false
		kv.mu.Unlock()
		return
	}
	if kv.config.Num < args.Version {
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d is waiting for the new config", kv.gid, kv.config.Num, kv.me)
		reply.Success = false
		kv.mu.Unlock()
		return
	}
	if kv.shards[args.ShardID] == Ready {
		Debug(dInfo, "---MigrateShards: [%d] [%d] Server %d has received the shard %d", kv.gid, kv.config.Num, kv.me, args.ShardID)
		reply.Success = true
		kv.mu.Unlock()
		return
	}
	//make the consensus
	ch := make(chan string)
	ErrCh := make(chan Err)
	index, _, isLeader := kv.rf.Start(Op{
		OpType:          "Receive",
		ResultMsg:       ch,
		ErrMsg:          ErrCh,
		From:            kv.me,
		ClientID:        args.ClientID,
		SequentID:       args.SequentID,
		Data:            args.Data,
		Version:         kv.config.Num,
		ShardID:         args.ShardID,
		DuplicatedTable: args.DuplicatedTable,
	})
	kv.mu.Unlock()
	if !isLeader {
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d is not leader", kv.gid, kv.config.Num, kv.me)
		reply.Success = false
		return
	}
	info := RequestInfo{
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
	}
	kv.requestTable[index] = &info

	select {
	case <-ch:
		reply.Success = true
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d success make the consensus", kv.gid, kv.config.Num, kv.me)
	case err := <-ErrCh:
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d failed make the consensus %v", kv.gid, kv.config.Num, kv.me, err)
	case <-time.After(RPCTimeout):
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d timeout make the consensus", kv.gid, kv.config.Num, kv.me)
	}
}

func (kv *ShardKV) handleUpdateMigrateState(op Op) {
	kv.mu.Lock()
	if kv.config.Num > op.Version {
		Debug(dInfo, "handleUpdateMigrateState: [%d] [%d] Server %d has higher or smaller version %v", kv.gid, kv.config.Num, kv.me, op.Version)
		kv.mu.Unlock()
		return
	}

	if kv.shards[op.ShardID] == WaitingMigrated {
		kv.shards[op.ShardID] = NotExist
		kv.storage[op.ShardID] = make(map[string]string)
	}

	kv.mu.Unlock()
}

// ----------------------------------Receive------------------------------
func (kv *ShardKV) receiveDaemon() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		shardToBeReceived := make([]int, 0)
		for shard, state := range kv.shards {
			if state == WaitingReceived {
				shardToBeReceived = append(shardToBeReceived, shard)
			}
		}
		if len(shardToBeReceived) != 0 {
			Debug(dInfo, "receiveDaemon: [%d] [%d] Leader %d is receiving shards %v", kv.gid, kv.config.Num, kv.me, shardToBeReceived)
			kv.receiving(shardToBeReceived)
		}
		kv.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
	}
}

func (kv *ShardKV) receiving(shards []int) {
	for _, shard := range shards {
		args := &ReceiveArgs{
			ShardID:   shard,
			Version:   kv.config.Num,
			ClientID:  kv.clientID,
			SequentID: kv.sequentID,
		}
		kv.sequentID++
		//call rpc from preConfig
		Debug(dTrace, "receiving: [%d] [%d] Leader %d: the preConfig Group is %v", kv.gid, kv.config.Num, kv.me, kv.preConfig.Groups[kv.preConfig.Shards[shard]])
		for _, server := range kv.preConfig.Groups[kv.preConfig.Shards[shard]] {
			//Debug(dTrace, "receiving: [%d] [%d] Leader %d: the server is %s", kv.gid, kv.config.Num, kv.me, server)
			go kv.handleReceiving(server, args, shard)
		}
	}
}

func (kv *ShardKV) handleReceiving(server string, args *ReceiveArgs, shard int) {
	var reply ReceiveReply
	srv := kv.make_end(server)
	ok := srv.Call("ShardKV.ReceiveShards", args, &reply)
	if ok && reply.Success {
		Debug(dInfo, "handleReceiving: [%d] [%d] Leader %d receive shard %v from %s success", kv.gid, kv.config.Num, kv.me, shard, server)
		kv.mu.Lock()
		//make the consensus
		ch := make(chan string)
		ErrCh := make(chan Err)
		index, _, isLeader := kv.rf.Start(Op{
			OpType:          "Receive",
			ShardID:         shard,
			Version:         kv.config.Num,
			Data:            reply.Data,
			DuplicatedTable: reply.DuplicatedTable,
			ClientID:        kv.clientID,
			SequentID:       kv.sequentID,
			ResultMsg:       ch,
			ErrMsg:          ErrCh,
		})
		kv.mu.Unlock()
		if !isLeader {
			return
		}
		kv.mu.Lock()
		kv.requestTable[index] = &RequestInfo{
			ClientID:  kv.clientID,
			SequentID: kv.sequentID,
		}
		kv.sequentID++
		kv.mu.Unlock()
		select {
		case <-ch:
			Debug(dInfo, "handleReceiving: [%d] [%d] Leader %d success make the consensus", kv.gid, kv.config.Num, kv.me)
		case err := <-ErrCh:
			Debug(dInfo, "handleReceiving: [%d] [%d] Leader %d failed make the consensus %v", kv.gid, kv.config.Num, kv.me, err)
		case <-time.After(RPCTimeout):
		}
	} else {
		Debug(dInfo, "handleReceiving: [%d] [%d] Leader %d receive shard %d from %s failed", kv.gid, kv.config.Num, kv.me, shard, server)
	}
}

// ReceiveShards when other servers call this RPC, it means current server need to reply the shard data to the other server
func (kv *ShardKV) ReceiveShards(args *ReceiveArgs, reply *ReceiveReply) {
	//Debug(dInfo, "ReceiveShards: [%d] [%d] Server %d receive shard request %d", kv.gid, kv.config.Num, kv.me, args.ShardID)
	kv.mu.Lock()
	if kv.config.Num < args.Version {
		Debug(dInfo, "ReceiveShards: [%d] [%d] Server %d has smaller version %v", kv.gid, kv.config.Num, kv.me, args.Version)
		reply.Success = false
		kv.mu.Unlock()
		return
	}

	//make the consensus
	ch := make(chan string)
	ErrCh := make(chan Err)
	index, _, isLeader := kv.rf.Start(Op{
		OpType:    "UpdateReceiveState",
		ShardID:   args.ShardID,
		Version:   args.Version,
		ResultMsg: ch,
		ErrMsg:    ErrCh,
		From:      kv.me,
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
	})
	kv.mu.Unlock()
	if !isLeader {
		Debug(dInfo, "ReceiveShards: [%d] [%d] Server %d is not leader", kv.gid, kv.config.Num, kv.me)
		reply.Success = false
		return
	}
	kv.mu.Lock()
	kv.requestTable[index] = &RequestInfo{
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
	}
	kv.mu.Unlock()
	select {
	case <-ch:
		Debug(dTrace, "ReceiveShards: [%d] [%d] Server %d shards %v", kv.gid, kv.config.Num, kv.me, kv.shards)
		reply.Data = DeepCopyStorageMap(kv.storage[args.ShardID])
		reply.DuplicatedTable = DeepCopyDuplicatedTableMap(kv.duplicatedTable[args.ShardID])
		reply.Success = true
		Debug(dInfo, "ReceiveShards: [%d] [%d] Server %d send shard %d success", kv.gid, kv.config.Num, kv.me, args.ShardID)
	case err := <-ErrCh:
		reply.Success = false
		Debug(dInfo, "ReceiveShards: [%d] [%d] Server %d send shard %d failed %v", kv.gid, kv.config.Num, kv.me, args.ShardID, err)
	case <-time.After(RPCTimeout):
		reply.Success = false
	}
}

func (kv *ShardKV) handleReceive(op Op) {
	kv.mu.Lock()
	if kv.config.Num > op.Version || kv.config.Num < op.Version {
		Debug(dInfo, "handleReceive: [%d] [%d] Server %d has higher or smaller version %v", kv.gid, kv.config.Num, kv.me, op.Version)
		kv.mu.Unlock()
		return
	}
	if kv.shards[op.ShardID] == WaitingReceived {
		kv.storage[op.ShardID] = DeepCopyStorageMap(op.Data)
		kv.duplicatedTable[op.ShardID] = DeepCopyDuplicatedTableMap(op.DuplicatedTable)
		kv.shards[op.ShardID] = Ready
		Debug(dInfo, "handleReceive: [%d] [%d] Server %d receive shard %d success", kv.gid, kv.config.Num, kv.me, op.ShardID)
		Debug(dInfo, "handleReceive: [%d] [%d] Server %d shards' %v", kv.gid, kv.config.Num, kv.me, kv.shards)
	}
	kv.mu.Unlock()
}

// it means that the leader receive the other obtaining request, if success, all server need to update the state
func (kv *ShardKV) handleUpdateReceiveState(op Op) {
	kv.mu.Lock()
	if kv.config.Num > op.Version {
		Debug(dInfo, "handleUpdateReceiveState: [%d] [%d] Server %d has higher or smaller version %v", kv.gid, kv.config.Num, kv.me, op.Version)
		kv.mu.Unlock()
		return
	}
	if kv.shards[op.ShardID] == WaitingMigrated {
		kv.shards[op.ShardID] = NotExist
		kv.storage[op.ShardID] = make(map[string]string)
	}

	kv.mu.Unlock()
}
