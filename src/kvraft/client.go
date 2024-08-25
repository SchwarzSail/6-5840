package kvraft

import (
	"crypto/rand"
	"math/big"

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

	for {
		reply := GetReply{}
		Debug(dInfo, "Client call the Get RPC, and ClientID is %d, SequentID is %d", ck.clientID, ck.sequentID)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			Debug(dInfo, "clerk success to get the reply of Get ,and clientid is %d, seqid is %d", args.ClientID, args.SequentID)
			ck.leaderID = i
			return reply.Value
		} else {
			if reply.Err == ErrRPCTimeout {
				Debug(dWarn, "RPC Get timeout,and clientid is %d, seqid is %d", args.ClientID, args.SequentID)
			}
			i = (i + 1) % len(ck.servers)
		}
	}

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

	for {
		reply := PutAppendReply{}
		Debug(dInfo, "Client call the PutAppend RPC, and ClientID is %d, SequentID is %d", ck.clientID, ck.sequentID)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			Debug(dInfo, "clerk success to get the reply of PutAppend ,and clientid is %d, seqid is %d", args.ClientID, args.SequentID)
			ck.leaderID = i
			return

		} else {
			if reply.Err == ErrRPCTimeout {
				Debug(dWarn, "RPC PutAppend timeout and clientid is %d, seqid is %d", args.ClientID, args.SequentID)
			}
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
