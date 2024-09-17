package shardkv

import (
	"6.5840/labgob"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
)

func (kv *ShardKV) persist(index int) {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(&kv.duplicatedTable) != nil || e.Encode(&kv.storage) != nil || e.Encode(&kv.shards) != nil || e.Encode(&kv.config) != nil {
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
	var storage map[int]map[string]string
	var duplicatedTable map[int64]LastReply

	var shards []State
	var cfg shardctrler.Config
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
	kv.config = cfg
	Debug(dSnap, "Server read snapshot success")
}
