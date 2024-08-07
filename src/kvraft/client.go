package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID  int
	clientID  int64
	sequentID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClientID:  ck.clientID,
		SequentID: ck.sequentID,
	}
	ck.sequentID++
	i := ck.leaderID
	now := time.Now()
	for  time.Since(now) < 10 * RPCTimeout{
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			Debug(dInfo, "clerk success to get the reply of Get")
			ck.leaderID = i
			return reply.Value
		} else {
			i = (i + 1) % len(ck.servers)
		}
	}
	Debug(dWarn, "clerk found that the rpc is timeout")
	//
	
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		ClientID:  ck.clientID,
		SequentID: ck.sequentID,
		Op:        op,
		Value:     value,
	}
	ck.sequentID++
	i := ck.leaderID
	now := time.Now()
	for  time.Since(now) < 10 * RPCTimeout{
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			Debug(dInfo, "clerk success to get the reply of PutAppend")
			ck.leaderID = i
			return

		} else {
			i = (i + 1) % len(ck.servers)
		}
	}
	//Debug(dWarn, "clerk found that the rpc is timeout")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
