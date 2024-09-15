package shardkv

import (
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrDuplicateReq    = "ErrDuplicateReq"
	ErrExpireReq       = "ErrExpireReq"
	ErrRPCTimeout      = "ErrRPCTimeout"
	ErrWrongRequest    = "ErrWrongRequest"
	ErrShardNotReady   = "ErrShardNotReady"
	ErrVersionNotMatch = "ErrVersionNotMatch"
)

const RPCTimeout = 5000 * time.Millisecond

type Err string

type State int

const (
	Ready State = iota
	WaitingReceived
	WaitingMigrated
	NotExist
)

type RequestInfo struct {
	ClientID  int64
	SequentID int
}

type DuplicatedKey struct {
	Shard    int
	ClientID int64
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	SequentID int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	SequentID int
}

type GetReply struct {
	Err   Err
	Value string
}

// ShardInfo use for key to grantee the uniqueness
type ShardInfo struct {
	ShardID int
	Version int
}

type ShardMigrationInfo struct {
	Args    *ShardMigrationArgs
	Servers []string
}

type ShardMigrationArgs struct {
	ShardID         int
	Num             int
	Storage         map[string]string
	DuplicatedTable map[DuplicatedKey]string
	ClientID        int64
	SequentID       int
	From            int
}

type ShardMigrationReply struct {
	Success bool
}

// migrating
type MigrationArgs struct {
	ShardID         int
	Version         int
	Data            map[string]string
	DuplicatedTable map[int64]string
	ClientID        int64
	SequentID       int
}

type MigrationReply struct {
	Success bool
}

// recevie
type ReceiveArgs struct {
	ShardID   int
	Version   int
	ClientID  int64
	SequentID int
}

type ReceiveReply struct {
	Success         bool
	Data            map[string]string
	DuplicatedTable map[int64]string
}
