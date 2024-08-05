package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDuplicateReq = "ErrDuplicateReq"
	ErrExpireReq = "ErrExpireReq"
	ErrRPCTimeout = "ErrRPCTimeout"
)
const (
	RPCTimeout = time.Second
)
type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	ClientID int64
	SequentID int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	SequentID int
}

type GetReply struct {
	Err   Err
	Value string
}
