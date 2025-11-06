package shardkv

// import "mitds/shardmaster"
import (
	"bytes"
	"log"
	"mitds/labgob"
	"mitds/labrpc"
	"mitds/raft"
	"mitds/shardmaster"
	"sync"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = 1

func Dprintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a)
	}
}

const (
	RespondTimeOut      = 500
	ConfigCheckInterval = 100
)

const (
	Get          = "Get"
	Put          = "Put"
	Append       = "Append"
	UpdateConfig = "UpdateConfig"
	GetShard     = "GetShard"
	GiveShard    = "GiveShard"
)

type ShardState int

const (
	Exist ShardState = iota
	NoExist
	WaitGet
	WaitGive
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId     int64
	ClientNum    int
	OpType       string
	Key          string
	Value        string
	Config       shardmaster.Config
	CfgNum       int
	ShardNum     int
	ShardData    map[string]string
	ShardSession map[int64]Session
}

type Session struct {
	ClientNum int
	Optype    string
	Response  Reply
}

type Reply struct {
	Err   Err
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead                  int32
	mck                   *shardmaster.Clerk
	kvdb                  map[int]map[string]string
	sessions              map[int64]Session
	notifyChan            map[int]chan Reply
	logLastApplied        int
	passiveSnapshotBefore bool
	shardStates           [shardmaster.NShards]ShardState
	preConfig             shardmaster.Config
	curConfig             shardmaster.Config
}

func (kv *ShardKV) checkKeyInGroup(key string) bool {
	shard := key2shard(key)
	if kv.shardStates[shard] != Exist {
		return false
	}

	return true
}

func (kv *ShardKV) createNotifyChan(index int) chan Reply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.notifyChan[index] = make(chan Reply)
	return kv.notifyChan[index]
}

func (kv *ShardKV) destroyNotifyChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if ch, ok := kv.notifyChan[index]; ok {
		close(ch)
		delete(kv.notifyChan, index)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.checkKeyInGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	op := Op{
		ClientId:  args.ClientId,
		ClientNum: args.ClientNum,
		OpType:    Get,
		Key:       args.Key,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.createNotifyChan(index)
	select {
	case res := <-ch:
		kv.mu.Lock()
		if !kv.checkKeyInGroup(args.Key) {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = OK
			reply.Value = res.Value
		}
		kv.mu.Unlock()
	case <-time.After(RespondTimeOut * time.Millisecond):
		reply.Err = ErrTimeOut
	}
	kv.destroyNotifyChan(index)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.checkKeyInGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if session, ok := kv.sessions[args.ClientId]; ok && args.ClientNum <= session.ClientNum {
		reply.Err = session.Response.Err
		kv.mu.Unlock()
		return
	} else {
		kv.mu.Unlock()

		op := Op{
			ClientId:  args.ClientId,
			ClientNum: args.ClientNum,
			OpType:    args.Op,
			Key:       args.Key,
			Value:     args.Value,
		}
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		ch := kv.createNotifyChan(index)
		select {
		case res := <-ch:
			kv.mu.Lock()
			if !kv.checkKeyInGroup(args.Key) {
				reply.Err = ErrWrongGroup
			} else {
				reply.Err = res.Err
			}
			kv.mu.Unlock()
		case <-time.After(RespondTimeOut * time.Millisecond):
			reply.Err = ErrTimeOut
		}
		kv.destroyNotifyChan(index)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	d := atomic.LoadInt32(&kv.dead)
	return d == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dead = 0
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.kvdb = make(map[int]map[string]string)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.kvdb[i] = make(map[string]string)
	}
	kv.sessions = make(map[int64]Session)
	kv.notifyChan = make(map[int]chan Reply)

	kv.logLastApplied = 0
	kv.passiveSnapshotBefore = false
	kv.preConfig = shardmaster.Config{}
	kv.curConfig = shardmaster.Config{}

	go kv.applyMessage()

	go kv.checkSnapshotNeed()

	go kv.getLatestConfig()

	go kv.checkGetShard()

	go kv.checkGiveShard()

	return kv
}

func (kv *ShardKV) executeNormalComand(op Op, index int, term int) {
	if !kv.checkKeyInGroup(op.Key) {
		return
	}

	reply := Reply{}
	if session, ok := kv.sessions[op.ClientId]; ok && op.OpType != Get && op.ClientNum <= session.ClientNum {
		reply.Err = session.Response.Err
	} else {
		shard := key2shard(op.Key)
		switch op.OpType {
		case Get:
			if val, ok := kv.kvdb[shard][op.Key]; ok {
				reply.Err = OK
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		case Put:
			kv.kvdb[shard][op.Key] = op.Value
			reply.Err = OK
		case Append:
			if val, ok := kv.kvdb[shard][op.Key]; ok {
				kv.kvdb[shard][op.Key] = val + op.Value
				reply.Err = OK
			} else {
				kv.kvdb[shard][op.Key] = op.Value
				reply.Err = ErrNoKey
			}
		}

		if op.OpType != Get {
			session := Session{
				ClientNum: op.ClientNum,
				Optype:    op.OpType,
				Response:  reply,
			}
			kv.sessions[op.ClientId] = session
		}
	}

	if ch, ok := kv.notifyChan[index]; ok {
		if t, isLeader := kv.rf.GetState(); isLeader && term == t {
			ch <- reply
		}
	}
}

func (kv *ShardKV) executeConfigCommand(op Op, index int, term int) {

}

func (kv *ShardKV) checkGetShard() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(ConfigCheckInterval * time.Millisecond)
			continue
		}

		shards := []int{}
		kv.mu.Lock()
		for i := 0; i < shardmaster.NShards; i++ {
			if kv.shardStates[i] == WaitGet {
				shards = append(shards, i)
			}
		}
		preConfig := kv.preConfig
		curCfgNum := kv.curConfig.Num
		kv.mu.Unlock()

		var wg sync.WaitGroup
		for _, shardNum := range shards {
			wg.Add(1)

			pregid := preConfig.Shards[shardNum]
			servers := preConfig.Groups[pregid]
			go func(servers []string, cfgNum int, shardNum int) {
				defer wg.Done()

				for _, server := range servers {
					args := MigrateShardArgs{
						CfgNum:   cfgNum,
						ShardNum: shardNum,
					}
					reply := MigrateShardReply{}
					srv := kv.make_end(server)
					ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
					if !ok || (ok && reply.Err == ErrWrongLeader) {
						continue
					}

					if ok && reply.Err == ErrNotReady {
						break
					}

					if ok && reply.Err == OK {
						op := Op{
							OpType:       GetShard,
							CfgNum:       cfgNum,
							ShardNum:     shardNum,
							ShardData:    reply.ShardData,
							ShardSession: reply.SessionData,
						}
						kv.rf.Start(op)
						break
					}
				}
			}(servers, curCfgNum, shardNum)
		}
		wg.Wait()
		time.Sleep(ConfigCheckInterval * time.Millisecond)
	}
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.curConfig.Num < args.CfgNum {
		reply.Err = ErrNotReady
		return
	}

	if kv.curConfig.Num > args.CfgNum {
		return
	}

	reply.Err = OK
	reply.ShardData = deepCopyMap(kv.kvdb[args.ShardNum])
	reply.SessionData = deepCopySession(kv.sessions)
}

func deepCopyMap(origin map[string]string) map[string]string {
	newmap := make(map[string]string)
	for key, value := range origin {
		newmap[key] = value
	}
	return newmap
}

func deepCopySession(origin map[int64]Session) map[int64]Session {
	newsession := make(map[int64]Session)
	for key, session := range origin {
		newsession[key] = session
	}

	return newsession
}

func (kv *ShardKV) checkGiveShard() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(ConfigCheckInterval * time.Millisecond)
			continue
		}

		shards := []int{}
		kv.mu.Lock()
		for i := 0; i < shardmaster.NShards; i++ {
			if kv.shardStates[i] == WaitGive {
				shards = append(shards, i)
			}
		}
		curConfig := kv.curConfig
		kv.mu.Unlock()

		var wg sync.WaitGroup
		for _, shardNum := range shards {
			wg.Add(1)

			curgid := curConfig.Shards[shardNum]
			servers := curConfig.Groups[curgid]
			go func(servers []string, cfgNum int, shardNum int) {
				defer wg.Done()
				for _, server := range servers {
					args := AckShardArgs{
						CfgNum:   cfgNum,
						ShardNum: shardNum,
					}
					Reply := AckShardReply{}
					srv := kv.make_end(server)
					ok := srv.Call("ShardKV.AckShard", &args, &Reply)

					if !ok || (ok && Reply.Err == ErrWrongLeader) {
						continue
					}

					if ok && Reply.Err == ErrNotReady {
						break
					}

					if ok && Reply.Err == OK && !Reply.Receive {
						break
					}

					if ok && Reply.Err == OK && Reply.Receive {
						op := Op{
							OpType:   GiveShard,
							CfgNum:   cfgNum,
							ShardNum: shardNum,
						}
						kv.rf.Start(op)
					}
				}
			}(servers, curConfig.Num, shardNum)
		}
		wg.Wait()
		time.Sleep(ConfigCheckInterval * time.Millisecond)
	}
}

func (kv *ShardKV) AckShard(args *AckShardArgs, reply *AckShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.curConfig.Num < args.CfgNum {
		reply.Err = ErrNotReady
		return
	}

	if kv.curConfig.Num > args.CfgNum {
		reply.Err = OK
		reply.Receive = true
		return
	}

	if kv.shardStates[args.ShardNum] == Exist {
		reply.Receive = true
	} else {
		reply.Receive = false
	}
	reply.Err = OK
}

func (kv *ShardKV) getLatestConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(ConfigCheckInterval * time.Millisecond)
			continue
		}

		if !kv.readyUpdateConfig() {
			time.Sleep(ConfigCheckInterval * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		curConfig := kv.curConfig
		kv.mu.Unlock()
		nextConfig := kv.mck.Query(curConfig.Num + 1)

		if nextConfig.Num == curConfig.Num+1 {
			op := Op{
				OpType: UpdateConfig,
				Config: nextConfig,
				CfgNum: nextConfig.Num,
			}

			kv.rf.Start(op)
		}

		time.Sleep(ConfigCheckInterval * time.Millisecond)
	}
}

func (kv *ShardKV) readyUpdateConfig() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i := 0; i < len(kv.shardStates); i++ {
		if kv.shardStates[i] != Exist || kv.shardStates[i] != NoExist {
			return false
		}
	}

	return true
}

func (kv *ShardKV) applyMessage() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.logLastApplied {
				kv.mu.Unlock()
				continue
			}

			if kv.passiveSnapshotBefore {
				if msg.CommandIndex != kv.logLastApplied+1 {
					kv.mu.Unlock()
					continue
				}
			}

			kv.logLastApplied = msg.CommandIndex
			op, ok := msg.Command.(Op)
			if ok {
				switch op.OpType {
				case Get, Put, Append:
					kv.executeNormalComand(op, msg.CommandIndex, msg.CommandTerm)
				case UpdateConfig, GetShard, GiveShard:
					kv.executeConfigCommand(op, msg.CommandIndex, msg.CommandTerm)
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.logLastApplied = msg.SnapshotIndex
			kv.applySnapshotToSM(msg.SnapShotData)
			kv.passiveSnapshotBefore = true
			kv.mu.Unlock()
			kv.rf.SetPassiveSnapshotFlag(false)
		}
	}
}

func (kv *ShardKV) applySnapshotToSM(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	var kvdb map[int]map[string]string
	var sessions map[int64]Session
	var shardStates [shardmaster.NShards]ShardState
	var preConfig shardmaster.Config
	var curConfig shardmaster.Config
	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)

	if d.Decode(&kvdb) != nil || d.Decode(&sessions) != nil || d.Decode(&shardStates) != nil ||
		d.Decode(&preConfig) != nil || d.Decode(&curConfig) != nil {
		Dprintf("[SHARD KV ERROR]: applySnapshotToSM failed")
	} else {
		kv.kvdb = kvdb
		kv.sessions = sessions
		kv.shardStates = shardStates
		kv.preConfig = preConfig
		kv.curConfig = curConfig
	}
}

func (kv *ShardKV) checkSnapshotNeed() {
	for !kv.killed() {
		if kv.rf.GetPassiveAndSetActiveFlag() {
			time.Sleep(ConfigCheckInterval * time.Millisecond)
			continue
		}

		var snapshotIndex int
		var snapshotData []byte
		if kv.maxraftstate != -1 && float32(kv.rf.GetRaftStateSize())/float32(kv.maxraftstate) > 0.9 {
			kv.mu.Unlock()
			snapshotIndex = kv.logLastApplied
			b := new(bytes.Buffer)
			e := labgob.NewEncoder(b)
			e.Encode(kv.kvdb)
			e.Encode(kv.sessions)
			e.Encode(kv.shardStates)
			e.Encode(kv.preConfig)
			e.Encode(kv.curConfig)
			snapshotData = b.Bytes()
			kv.mu.Unlock()
		}

		if snapshotData != nil {
			kv.rf.SnapShot(snapshotIndex, snapshotData)
		}

		kv.rf.SetActiveSnapshotFlag(false)
		time.Sleep(ConfigCheckInterval * time.Millisecond)
	}
}
