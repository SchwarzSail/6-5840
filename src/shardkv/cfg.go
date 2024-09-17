package shardkv

import (
	"6.5840/shardctrler"
	"time"
)

func (kv *ShardKV) processMonitor() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		if !kv.isAllUpdated() {
			Debug(dInfo, "processMonitor: [%d] [%d] Server %d is waiting for all shards to be updated, shards %v", kv.gid, kv.config.Num, kv.me, kv.shards)
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		currentCfg := kv.config
		kv.mu.Unlock()
		newConfig := kv.mck.Query(currentCfg.Num + 1)
		if currentCfg.Num+1 == newConfig.Num {
			Debug(dInfo, "processMonitor: [%d] [%d] Leader %d find the new config %v", kv.gid, currentCfg.Num, kv.me, newConfig)
			kv.rf.Start(Op{
				OpType:    "UpdateConfig",
				NewConfig: &newConfig,
				Version:   newConfig.Num,
			})
		}
	}

}

func (kv *ShardKV) handleUpdateConfig(op Op) {
	if kv.config.Num+1 != op.Version {
		Debug(dInfo, "handleUpdateConfig: [%d] [%d] Server %d find that the version is not next one", kv.gid, kv.config.Num, kv.me)
		return
	}
	if !kv.isAllUpdated() {
		Debug(dInfo, "handleUpdateConfig: [%d] [%d] Server %d is waiting for all shards to be updated", kv.gid, kv.config.Num, kv.me)
		return
	}

	for shard := 0; shard < shardctrler.NShards; shard++ {
		if op.NewConfig.Shards[shard] != kv.gid && kv.shards[shard] == Ready {
			kv.shards[shard] = WaitingMigrated
		} else if op.NewConfig.Shards[shard] == kv.gid && kv.shards[shard] == NotExist {
			kv.shards[shard] = WaitingReceived
		}
	}
	kv.config = *op.NewConfig
	Debug(dInfo, "handleUpdateConfig: [%d] [%d] Server %d update the config to %v", kv.gid, kv.config.Num, kv.me, kv.config)

}

// check whether all shards are updated
// need to be called under the lock
func (kv *ShardKV) isAllUpdated() bool {
	for _, state := range kv.shards {
		if state == WaitingMigrated || state == WaitingReceived {
			return false
		}
	}
	return true
}
