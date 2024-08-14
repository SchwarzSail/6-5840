package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

func (kv *KVServer) persist(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplied)
	e.Encode(kv.clientTable)
	e.Encode(kv.storage)
	raftstate := w.Bytes()
	kv.rf.Snapshot(index, raftstate)
}

func (kv *KVServer) readFromSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var clientTable map[int64]int
	var storage map[string]string
	if d.Decode(&lastApplied) != nil || d.Decode(&clientTable) != nil || d.Decode(&storage) != nil {
		panic("read from snapshot failed")
	}
	//kv.mu.Lock()
	//defer kv.mu.Lock()
	kv.lastApplied = lastApplied
	kv.clientTable = clientTable
	kv.storage = storage
	Debug(dSnap,"Server read snapshot success")
}