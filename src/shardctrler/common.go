package shardctrler

import "time"

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}


func(cfg *Config) clone() *Config {
	res := &Config{
		Num: cfg.Num,
		Groups: make(map[int][]string),
	}
	copy(res.Shards[:], cfg.Shards[:])
	for k, v := range cfg.Groups {
		temp := make([]string, len(v))
		copy(temp, v)
		res.Groups[k] = temp
	}
	return res
}

const RPCTimeout = 5000 * time.Millisecond

const (
	OK = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongRequest = "ErrWrongRequest"
	ErrExpireReq = "ErrExpiredReq"
	ErrRPCTimeout = "ErrRPCTimeout"
)

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClientID  int64
	SequentID int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	ClientID  int64
	SequentID int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClientID  int64
	SequentID int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClientID  int64
	SequentID int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
