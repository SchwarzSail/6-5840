package kvsrv

import (
	"log"
	"sync"
)

var RequestCount int

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	storage map[string]string

	client map[int]map[int]PutAppendReply //保存用户的每一次操作，确保重复的request只会被执行一次
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.storage[args.Key]

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.storage[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//确保缓存了用户
	if _, ok := kv.client[args.ClientID]; !ok {
		kv.client[args.ClientID] = make(map[int]PutAppendReply)
	}
	if val, ok := kv.client[args.ClientID][args.RequestID]; !ok {
		kv.client[args.ClientID][args.RequestID] = PutAppendReply{Value: kv.storage[args.Key]}
		reply.Value = kv.storage[args.Key]
		kv.storage[args.Key] += args.Value
	} else {
		//log.Printf("the same request")
		reply.Value = val.Value
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.client = make(map[int]map[int]PutAppendReply)
	kv.mu = sync.Mutex{}
	return kv
}

func (kv *KVServer) FreeMemory(args *FreeMemoryArgs, reply *FreeMemoryReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.client[args.ClientID], args.RequestID)
}
