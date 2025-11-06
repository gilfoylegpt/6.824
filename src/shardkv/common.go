package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeout"
	ErrNotReady    = "NotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	ClientNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	ClientNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateShardArgs struct {
	CfgNum   int
	ShardNum int
}

type MigrateShardReply struct {
	Err         Err
	ShardData   map[string]string
	SessionData map[int64]Session
}

type AckShardArgs struct {
	CfgNum   int
	ShardNum int
}

type AckShardReply struct {
	Err     Err
	Receive bool
}
