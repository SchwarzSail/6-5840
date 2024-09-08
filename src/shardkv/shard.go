package shardkv

import (
	"6.5840/shardctrler"
	"time"
)

func (kv *ShardKV) migrateData(args *ShardMigrationArgs) {
	for k, v := range args.Storage {
		kv.storage[k] = v
	}
	for k, v := range args.DuplicatedTable {
		kv.duplicatedTable[k] = v
	}
}

func (kv *ShardKV) processMonitor() {
	for !kv.killed() {
		kv.mu.Lock()
		currentConfig := kv.cfg
		newConfig := kv.mck.Query(-1)
		if newConfig.Num != currentConfig.Num {
			for i := currentConfig.Num + 1; i <= newConfig.Num; i++ {
				cfg := kv.mck.Query(i)
				Debug(dTrace, "processMonitor: Server %d find the new config whose version is %d", kv.me, cfg.Num)
				kv.rf.Start(Op{
					OpType:    "UpdateConfig",
					NewConfig: &cfg,
					From:      kv.me,
				})
			}
		}
		kv.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
	}
}

func (kv *ShardKV) processSendingShards() {
	for !kv.killed() {
		kv.mu.Lock()
		for len(kv.shardToBeSentTable) == 0 {
			kv.cond.Wait()
		}
		Debug(dInfo, "processSendingShards: The shardToBeSentTable is %v", kv.shardToBeSentTable)
		for _, data := range kv.shardToBeSentTable {
			go kv.migratingShards(data)
		}
		kv.mu.Unlock()
		time.Sleep(400 * time.Millisecond)
	}
}

// call RPCs
func (kv *ShardKV) migratingShards(info ShardMigrationInfo) {
	Debug(dTrace, "migratingShards: Server %d start to migrate the shard data whose info's ShardID is %d, and version is %d, data comes from %d", kv.me, info.Args.ShardID, info.Args.Num, info.Args.From)
	for _, i := range info.Servers {
		server := kv.make_end(i)
		var reply ShardMigrationReply
		ok := server.Call("ShardKV.MigrateShards", info.Args, &reply)
		if ok && reply.Success {
			kv.mu.Lock()
			Debug(dTrace, "migratingShards: Server %d success to get the reply of RPC, whose version is %d", kv.me, info.Args.Num)
			key := ShardInfo{
				ShardID: info.Args.ShardID,
				Version: info.Args.Num,
			}
			delete(kv.shardToBeSentTable, key)
			//Debug(dTrace, "migratingShards: Server %d find that the shardToBeSentTable is %v", kv.me, kv.shardToBeSentTable)
			kv.mu.Unlock()
		}
	}
}

// only leader can handle this RPC request
func (kv *ShardKV) MigrateShards(args *ShardMigrationArgs, reply *ShardMigrationReply) {
	if args.Num < kv.cfg.Num {
		reply.Success = true
		return
	}

	ch := make(chan string, 1)
	errCh := make(chan Err, 1)
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{
		OpType:        "ReceiveShards",
		MigrationArgs: args,
		From:          kv.me,
		ResultMsg:     ch,
		ErrMsg:        errCh,
		ClientID:      args.ClientID,
		SequentID:     args.SequentID,
	})
	kv.mu.Unlock()
	if !isLeader {
		reply.Success = false
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
		reply.Success = true
		Debug(dInfo, "MigrateShards: Server %d receive the shard data whose version is %d", kv.me, args.Num)
	case err := <-errCh:
		reply.Success = false
		Debug(dWarn, "MigrateShards: The Err is %v", err)
	case <-time.After(RPCTimeout):
		Debug(dWarn, "MigrateShards: Server %d find that the RPC is timeout, and ClientID is %d, SequentID is %d", kv.me, args.ClientID, args.SequentID)
		reply.Success = false
	}
}

func (kv *ShardKV) migrateShards(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.cfg.Num > op.MigrationArgs.Num {
		return
	}
	Debug(dInfo, "migrateShards: Server %d start to migrate the shard data whose version is %d, and gid is %d", kv.me, op.MigrationArgs.Num, op.MigrationArgs.From)
	//grantee the gid is equal to the current server's gid
	if kv.cfg.Shards[op.MigrationArgs.ShardID] == kv.gid {
		if from, ok := kv.shardTobeReceived[op.MigrationArgs.ShardID]; ok {
			if op.MigrationArgs.Num != kv.cfg.Num {
				Debug(dTrace, "migrateShards: Server %d find that the version %d is not equal to the current config's version, which is %d", kv.me, op.MigrationArgs.Num, kv.cfg.Num)
				return
			}
			if from != op.MigrationArgs.From {
				panic("")
			}
			Debug(dTrace, "migrateShards: Server %d apply the shard data whose version is %d, and gid is %d", kv.me, op.MigrationArgs.Num, op.MigrationArgs.From)
			kv.migrateData(op.MigrationArgs)
			delete(kv.shardTobeReceived, op.MigrationArgs.ShardID)
			return
		}
	}
	key := ShardInfo{
		ShardID: op.MigrationArgs.ShardID,
		Version: op.MigrationArgs.Num,
	}
	Debug(dTrace, "--------migrateShards: Server %d save the cache data whose version is %d, and gid is %d", kv.me, op.MigrationArgs.Num, op.MigrationArgs.From)
	kv.shardRequestCache[key] = op.MigrationArgs
}

func (kv *ShardKV) applyCacheData(newConfig *shardctrler.Config) {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.cfg.Shards[i] != 0 {
			//it is the new shards that the current server  never work for it before, so we can apply it
			if kv.cfg.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				key := ShardInfo{
					ShardID: i,
					Version: newConfig.Num,
				}
				if args, ok := kv.shardRequestCache[key]; ok {
					Debug(dTrace, "applyCacheData: Server %d find the cache data whose version is %d, and server its version is %d", kv.me, args.Num, kv.cfg.Num)
					kv.migrateData(args)
					delete(kv.shardRequestCache, key)
				} else {
					Debug(dTrace, "applyCacheData: Server %d find doesn't find the cache data , expect from %d, and version is %d", kv.me, kv.cfg.Shards[i], newConfig.Num)
					kv.shardTobeReceived[i] = kv.cfg.Shards[i]
				}
			}
		}
	}
}

func (kv *ShardKV) updateConfig(op Op) {
	kv.mu.Lock()
	// check the config message
	if kv.cfg.Num+1 != op.NewConfig.Num {
		Debug(dTrace, "updateConfig: Server %d find that the new config's version is not the next version. The kv's version is %d, and newConfig's version is %d", kv.me, kv.cfg.Num, op.NewConfig.Num)
		kv.mu.Unlock()
		return
	}
	if len(kv.shardTobeReceived) != 0 {
		Debug(dTrace, "updateConfig: Server %d find that the shardTobeReceived is not empty, which is %v", kv.me, kv.shardTobeReceived)
		kv.mu.Unlock()
		return
	}
	// grantee the current server is leader, if not ,we need to apply the cache config immediately
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		Debug(dTrace, "updateConfig: Follower %d start to apply the cache config", kv.me)
		kv.applyCacheData(op.NewConfig)
		kv.cfg = op.NewConfig
		kv.mu.Unlock()
		return
	}
	//prepare the data that need to be migrated
	shardsMigration := make([]int, 0, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.cfg.Shards[i] == kv.gid && op.NewConfig.Shards[i] != kv.gid {
			shardsMigration = append(shardsMigration, i)
		}
	}
	kv.applyCacheData(op.NewConfig)
	if len(shardsMigration) != 0 {
		kv.prepareMigratingShards(shardsMigration, op.NewConfig)
	}
	kv.cfg = op.NewConfig
	Debug(dInfo, "updateConfig: Server %d updated config to version %d", kv.me, kv.cfg.Num)
	kv.mu.Unlock()
}

// prepare the args to send
func (kv *ShardKV) prepareMigratingShards(shards []int, newConfig *shardctrler.Config) {
	shardData := make(map[int]ShardMigrationInfo)
	for _, shard := range shards {
		args := &ShardMigrationArgs{
			ShardID:         shard,
			Num:             newConfig.Num,
			Storage:         make(map[string]string),
			DuplicatedTable: make(map[DuplicatedKey]string),
			ClientID:        kv.clientID,
			SequentID:       kv.sequentID,
			From:            kv.gid,
		}
		kv.sequentID++
		shardData[shard] = ShardMigrationInfo{
			Args:    args,
			Servers: newConfig.Groups[newConfig.Shards[shard]],
		}
	}

	//deep copy
	for k, v := range kv.storage {
		shard := key2shard(k)
		if data, ok := shardData[shard]; ok {
			Debug(dTrace, "prepareMigratingShards: Server %d send that the key is %v", kv.me, k)
			data.Args.Storage[k] = v
			delete(kv.storage, k)
		}
	}

	for k, v := range kv.duplicatedTable {
		for _, shard := range shards {
			if shard == k.Shard {
				Debug(dTrace, "prepareMigratingShards: Server %d find that the duplicated key is %v", kv.me, k)
				shardData[shard].Args.DuplicatedTable[k] = v
				delete(kv.duplicatedTable, k)
			}
		}
	}

	//send the args
	for _, data := range shardData {
		key := ShardInfo{
			ShardID: data.Args.ShardID,
			Version: data.Args.Num,
		}
		kv.shardToBeSentTable[key] = data
	}
	kv.cond.Broadcast()
}
