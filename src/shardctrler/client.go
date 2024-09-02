package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	clientID   int64
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
	ck.clientID = nrand()
	return ck
}


func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.ClientID = ck.clientID
	args.SequentID = ck.sequentID
	ck.sequentID++
	args.Num = num
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK{
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.ClientID = ck.clientID
	args.SequentID = ck.sequentID
	ck.sequentID++
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK{
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	args.ClientID = ck.clientID
	args.SequentID = ck.sequentID
	ck.sequentID++
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false  && reply.Err == OK{
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.ClientID = ck.clientID
	args.SequentID = ck.sequentID
	ck.sequentID++
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false  && reply.Err == OK{
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
