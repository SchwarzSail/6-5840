package shardkv

import (
	"6.5840/labgob"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
)

func (kv *ShardKV) persist(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	cfg := kv.config.Load()
	if e.Encode(&kv.duplicatedTable) != nil || e.Encode(&kv.storage) != nil || e.Encode(&kv.shards) != nil || e.Encode(&cfg) != nil {
		panic(fmt.Sprintf("[%d] [%d] Server %d failed to encode the statement", kv.gid, kv.config.Load().Num, kv.me))
	}

	raftstate := w.Bytes()
	kv.rf.Snapshot(index, raftstate)
}

func (kv *ShardKV) readFromSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var storage map[int]map[string]string
	var duplicatedTable map[int64]LastReply

	var shards []State
	var cfg *shardctrler.Config
	if err := d.Decode(&duplicatedTable); err != nil {
		panic(err)
	}
	if err := d.Decode(&storage); err != nil {
		panic(err)
	}
	if err := d.Decode(&shards); err != nil {
		panic(err)
	}
	if err := d.Decode(&cfg); err != nil {
		panic(err)
	}

	kv.storage = storage
	kv.duplicatedTable = duplicatedTable
	kv.shards = shards
	kv.config.Store(cfg)
	//for current3B
	//if server restart and read snapshot, the previous log will be persisted in log entries.
	//because there is no new request arrived, the leader can't commit the log whose term is not equal to leader's term.
	//it leads to server will be pending in the loop of waiting for shard data, but the shard data command is in the log entries.
	//so we need to append the empty log to make leader can commit the log whose term is equal to leader's term.
	kv.rf.Start(Op{
		OpType: "Nothing",
	})
	Debug(dSnap, "[%d] [%d] Server %d read snapshot success", kv.gid, cfg.Num, kv.me)
}
