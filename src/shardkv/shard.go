package shardkv

import (
	"time"
)

func DeepCopyStorageMap(src map[string]string) map[string]string {
	dst := make(map[string]string)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func DeepCopyDuplicatedTableMap(src map[int64]LastReply) map[int64]LastReply {
	dst := make(map[int64]LastReply)
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
			Debug(dInfo, "migratingDaemon: [%d] [%d] Leader %d is migrating shards %v", kv.gid, kv.config.Load().Num, kv.me, shardToBeMigrated)
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
			Version:         kv.config.Load().Num,
			Data:            make(map[string]string),
			DuplicatedTable: make(map[int64]LastReply),
			ClientID:        kv.clientID,
			SequentID:       kv.sequentID,
		}
		kv.sequentID++
		args.Data = DeepCopyStorageMap(kv.storage[shard])
		args.DuplicatedTable = DeepCopyDuplicatedTableMap(kv.duplicatedTable)
		//Call RPCs
		Debug(dTrace, "migrating: [%d] [%d] Leader %d: the config  Group is %v", kv.gid, kv.config.Load().Num, kv.me, kv.config.Load().Groups[kv.config.Load().Shards[shard]])
		for _, server := range kv.config.Load().Groups[kv.config.Load().Shards[shard]] {
			go kv.handleMigrating(server, args, shard)
		}
	}
}

func (kv *ShardKV) handleMigrating(server string, args *MigrationArgs, shard int) {
	var reply MigrationReply
	srv := kv.make_end(server)
	ok := srv.Call("ShardKV.MigrateShards", args, &reply)
	if ok && reply.Success {
		Debug(dInfo, "handleMigrating: [%d] [%d] Leader %d migrate shard %d to %s success", kv.gid, kv.config.Load().Num, kv.me, shard, server)
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
			Debug(dInfo, "handleMigrating: [%d] [%d] Leader %d success make the consensus", kv.gid, kv.config.Load().Num, kv.me)
		case err := <-ErrCh:
			Debug(dInfo, "handleMigrating: [%d] [%d] Leader %d failed make the consensus %v", kv.gid, kv.config.Load().Num, kv.me, err)
		case <-time.After(RPCTimeout):
			Debug(dInfo, "handleMigrating: [%d] [%d] Leader %d timeout make the consensus", kv.gid, kv.config.Load().Num, kv.me)
		}
	} else {
		Debug(dInfo, "handleMigrating: [%d] [%d] Leader %d migrate shard %d to %s failed", kv.gid, kv.config.Load().Num, kv.me, shard, server)
	}
}

// MigrateShards other send shards to current server
func (kv *ShardKV) MigrateShards(args *MigrationArgs, reply *MigrationReply) {
	//check the version
	kv.mu.Lock()
	if kv.config.Load().Num < args.Version {
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d has  smaller version %v", kv.gid, kv.config.Load().Num, kv.me, args.Version)
		reply.Success = false
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
		Version:         kv.config.Load().Num,
		ShardID:         args.ShardID,
		DuplicatedTable: args.DuplicatedTable,
	})
	if !isLeader {
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d is not leader", kv.gid, kv.config.Load().Num, kv.me)
		reply.Success = false
		kv.mu.Unlock()
		return
	}
	info := RequestInfo{
		ClientID:  args.ClientID,
		SequentID: args.SequentID,
	}
	kv.requestTable[index] = &info
	kv.mu.Unlock()

	select {
	case <-ch:
		reply.Success = true
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d success make the consensus", kv.gid, kv.config.Load().Num, kv.me)
	case err := <-ErrCh:
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d failed make the consensus %v", kv.gid, kv.config.Load().Num, kv.me, err)
	case <-time.After(RPCTimeout):
		Debug(dInfo, "MigrateShards: [%d] [%d] Server %d timeout make the consensus", kv.gid, kv.config.Load().Num, kv.me)
	}

}

func (kv *ShardKV) handleUpdateMigrateState(op Op) {
	currentConfig := kv.config.Load()
	if currentConfig.Num > op.Version || currentConfig.Num < op.Version {
		Debug(dInfo, "handleUpdateMigrateState: [%d] [%d] Server %d has higher or smaller version %v", kv.gid, currentConfig.Num, kv.me, op.Version)
		return
	}
	if kv.shards[op.ShardID] == WaitingMigrated {
		kv.shards[op.ShardID] = NotExist
		kv.storage[op.ShardID] = make(map[string]string)
	}
}

func (kv *ShardKV) handleReceive(op Op) {
	currentConfig := kv.config.Load()

	if currentConfig.Num > op.Version || currentConfig.Num < op.Version {
		Debug(dInfo, "handleReceive: [%d] [%d] Server %d has higher or smaller version %v", kv.gid, currentConfig.Num, kv.me, op.Version)
		return
	}
	if kv.shards[op.ShardID] == WaitingReceived {
		for k, v := range op.Data {
			kv.storage[op.ShardID][k] = v
		}
		for clientID, lastReply := range op.DuplicatedTable {
			if temp, ok := kv.duplicatedTable[clientID]; !ok || temp.SequentID <= lastReply.SequentID {
				kv.duplicatedTable[clientID] = lastReply
			}
		}
		kv.shards[op.ShardID] = Ready
		Debug(dInfo, "handleReceive: [%d] [%d] Server %d receive shard %d success", kv.gid, kv.config.Load().Num, kv.me, op.ShardID)
		Debug(dInfo, "handleReceive: [%d] [%d] Server %d shards' %v", kv.gid, kv.config.Load().Num, kv.me, kv.shards)
	}
}
