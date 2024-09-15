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
	if e.Encode(&kv.clientTable) != nil || e.Encode(&kv.duplicatedTable) != nil || e.Encode(&kv.storage) != nil || e.Encode(&kv.shards) != nil || e.Encode(&kv.config) != nil || e.Encode(&kv.preConfig) != nil {
		panic(fmt.Sprintf("[%d] [%d] Server %d failed to encode the statement", kv.gid, kv.config.Num, kv.me))
	}

	raftstate := w.Bytes()
	kv.rf.Snapshot(index, raftstate)
}

func (kv *ShardKV) readFromSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clientTable map[int64]int
	var storage map[int]map[string]string
	var duplicatedTable map[int]map[int64]string

	var shards []State
	var cfg shardctrler.Config
	var preCfg shardctrler.Config
	if err := d.Decode(&clientTable); err != nil {
		panic(err)
	}
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
	if err := d.Decode(&preCfg); err != nil {
		panic(err)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.clientTable = clientTable
	kv.storage = storage
	kv.duplicatedTable = duplicatedTable

	kv.shards = shards
	kv.config = cfg
	kv.preConfig = preCfg
	Debug(dSnap, "Server read snapshot success")
}
