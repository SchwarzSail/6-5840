package shardkv

import (
	"bytes"
	"fmt"
	"6.5840/labgob"
)

func (kv *ShardKV) persist(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(&kv.clientTable) != nil || e.Encode(&kv.duplicatedTable) != nil|| e.Encode(&kv.storage) != nil {
		panic(fmt.Sprintf("Server %d failed to encode the statement", kv.me))
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
	var storage map[string]string
	var duplicatedTable map[int64]string
	if err := d.Decode(&clientTable); err != nil {
		panic(err)
	}
	if err := d.Decode(&duplicatedTable); err != nil {
		panic(err)
	}
	if err := d.Decode(&storage); err != nil {
		panic(err)
	}
	// if d.Decode(&lastApplied) != nil || d.Decode(&clientTable) != nil || d.Decode(&duplicatedTable) != nil || d.Decode(&kv.storage) != nil {
	// 	panic(fmt.Sprintf("Server %d read from snapshot failed",kv.me))
	// }
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.clientTable = clientTable
	kv.storage = storage
	kv.duplicatedTable = duplicatedTable
	Debug(dSnap,"Server read snapshot success")
}
