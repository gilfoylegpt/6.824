package shardmaster

import (
	"mitds/labgob"
	"mitds/labrpc"
	"mitds/raft"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Query = "Query"
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
)

const RespondTimeOut = 500

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	sessions   map[int64]Session
	configs    []Config // indexed by config num
	notifyChan map[int]chan Reply
}

type Session struct {
	ClientNum int
	OpType    string
	Response  Reply
}

type Reply struct {
	Err    Err
	Config Config
}

type Op struct {
	// Your data here.
	ClientId  int64
	ClientNum int
	OpType    string
	Servers   map[int][]string
	Groups    []int
	Shard     int
	GID       int
	CfgNum    int
}

func (sm *ShardMaster) createNotifyChan(index int) chan Reply {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.notifyChan[index] = make(chan Reply, 1)
	return sm.notifyChan[index]
}

func (sm *ShardMaster) destroyNotifyChan(index int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if ch, ok := sm.notifyChan[index]; ok {
		close(ch)
		delete(sm.notifyChan, index)
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()
	if session, ok := sm.sessions[args.ClientId]; ok && args.ClientNum <= session.ClientNum {
		reply.Err = session.Response.Err
		sm.mu.Unlock()
	} else {
		sm.mu.Unlock()

		op := Op{
			ClientId:  args.ClientId,
			ClientNum: args.ClientNum,
			OpType:    Join,
			Servers:   args.Servers,
		}
		index, _, isLeader := sm.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		ch := sm.createNotifyChan(index)
		select {
		case res := <-ch:
			reply.Err = res.Err
		case <-time.After(RespondTimeOut * time.Millisecond):
			reply.Err = ErrTimeOut
		}
		sm.destroyNotifyChan(index)
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()
	if session, ok := sm.sessions[args.ClientId]; ok && args.ClientNum <= session.ClientNum {
		reply.Err = session.Response.Err
		sm.mu.Unlock()
	} else {
		sm.mu.Unlock()

		op := Op{
			ClientId:  args.ClientId,
			ClientNum: args.ClientNum,
			OpType:    Leave,
			Groups:    args.GIDs,
		}
		index, _, isLeader := sm.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		ch := sm.createNotifyChan(index)
		select {
		case res := <-ch:
			reply.Err = res.Err
		case <-time.After(RespondTimeOut * time.Millisecond):
			reply.Err = ErrTimeOut
		}
		sm.destroyNotifyChan(index)
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()
	if session, ok := sm.sessions[args.ClientId]; ok && args.ClientNum <= session.ClientNum {
		reply.Err = session.Response.Err
		sm.mu.Unlock()
	} else {
		sm.mu.Unlock()

		op := Op{
			ClientId:  args.ClientId,
			ClientNum: args.ClientNum,
			OpType:    Move,
			Shard:     args.Shard,
			GID:       args.GID,
		}
		index, _, isLeader := sm.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		ch := sm.createNotifyChan(index)
		select {
		case res := <-ch:
			reply.Err = res.Err
		case <-time.After(RespondTimeOut * time.Millisecond):
			reply.Err = ErrTimeOut
		}
		sm.destroyNotifyChan(index)
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		ClientNum: args.ClientNum,
		OpType:    Query,
		CfgNum:    args.Num,
	}
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := sm.createNotifyChan(index)
	select {
	case res := <-ch:
		reply.Err = res.Err
		reply.Config = res.Config
	case <-time.After(RespondTimeOut * time.Millisecond):
		reply.Err = ErrTimeOut
	}
	sm.destroyNotifyChan(index)
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	d := atomic.LoadInt32(&sm.dead)
	return d == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) applyConfigChange() {
	for !sm.killed() {
		msg := <-sm.applyCh
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if ok {
				reply := Reply{}
				sm.mu.Lock()
				if session, ok := sm.sessions[op.ClientId]; ok && op.OpType != Query && op.ClientNum <= session.ClientNum {
					reply.Err = session.Response.Err
				} else {
					switch op.OpType {
					case Join:
						reply.Err = sm.executeJoin(op)
					case Leave:
						reply.Err = sm.executeLeave(op)
					case Move:
						reply.Err = sm.executeMove(op)
					case Query:
						reply.Err, reply.Config = sm.executeQuery(op)
					}

					if op.OpType != Query {
						session := Session{
							ClientNum: op.ClientNum,
							OpType:    op.OpType,
							Response:  reply,
						}
						sm.sessions[op.ClientId] = session
					}
				}
				ch, ok := sm.notifyChan[msg.CommandIndex]
				sm.mu.Unlock()
				if ok {
					if term, isLeader := sm.rf.GetState(); isLeader && term == msg.CommandTerm {
						ch <- reply
					}
				}
			}
		}
	}
}

func (sm *ShardMaster) getLatestConfig() Config {
	return sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) executeJoin(op Op) Err {
	latestConfig := sm.getLatestConfig()
	newConfig := Config{}
	newConfig.Num = latestConfig.Num + 1

	newGroups := deepCopyGroups(latestConfig.Groups)
	for gid, servers := range op.Servers {
		newGroups[gid] = servers
	}
	newConfig.Groups = newGroups

	newConfig.Shards = shardLoadBalance(newGroups, latestConfig.Shards)

	sm.configs = append(sm.configs, newConfig)

	return OK
}

func (sm *ShardMaster) executeLeave(op Op) Err {
	latestConfig := sm.getLatestConfig()
	newConfig := Config{}
	newConfig.Num = latestConfig.Num + 1

	newGroups := deepCopyGroups(latestConfig.Groups)
	for _, gid := range op.Groups {
		delete(newGroups, gid)
	}
	newConfig.Groups = newGroups

	if len(newGroups) != 0 {
		newConfig.Shards = shardLoadBalance(newGroups, latestConfig.Shards)
	}

	sm.configs = append(sm.configs, newConfig)

	return OK
}

func shardLoadBalance(newgroups map[int][]string, originShard [NShards]int) [NShards]int {
	newShard := originShard
	shardCnt := map[int]int{}

	for idx, gid := range newShard {
		if _, ok := newgroups[gid]; ok {
			shardCnt[gid]++
		} else {
			newShard[idx] = 0
		}
	}

	gids := []int{}
	for gid, _ := range newgroups {
		gids = append(gids, gid)
		if _, ok := shardCnt[gid]; !ok {
			shardCnt[gid] = 0
		}
	}

	average := NShards / len(newgroups)
	remainer := NShards % len(newgroups)

	for i := 0; i < len(gids)-1; i++ {
		for j := len(gids) - 1; j > i; j-- {
			if shardCnt[gids[j]] > shardCnt[gids[j-1]] || (shardCnt[gids[j]] == shardCnt[gids[j-1]] && gids[j] < gids[j-1]) {
				// gids[j], gids[j-1] = gids[j-1], gids[j]
				gids[j-1], gids[j] = gids[j], gids[j-1]
			}
		}
	}

	for i := 0; i < len(gids); i++ {
		var curTar int
		if i < remainer {
			curTar = average + 1
		} else {
			curTar = average
		}
		delta := shardCnt[gids[i]] - curTar
		if delta == 0 {
			continue
		}

		if delta > 0 {
			for j := 0; j < NShards; j++ {
				if delta == 0 {
					break
				}

				if newShard[j] == gids[i] {
					newShard[j] = 0
					delta--
				}
			}
		}
	}

	for i := 0; i < len(gids); i++ {
		var curTar int
		if i < remainer {
			curTar = average + 1
		} else {
			curTar = average
		}
		delta := shardCnt[gids[i]] - curTar
		if delta == 0 {
			continue
		}

		if delta < 0 {
			for j := 0; j < NShards; j++ {
				if delta == 0 {
					break
				}
				if newShard[j] == 0 {
					newShard[j] = gids[i]
					delta++
				}
			}
		}
	}

	return newShard
}

func (sm *ShardMaster) executeMove(op Op) Err {
	latestConfg := sm.getLatestConfig()
	newConfig := Config{}
	newConfig.Num = latestConfg.Num + 1

	newConfig.Groups = deepCopyGroups(latestConfg.Groups)
	newShard := latestConfg.Shards
	newShard[op.Shard] = op.GID
	newConfig.Shards = newShard

	sm.configs = append(sm.configs, newConfig)

	return OK
}

func deepCopyGroups(origin map[int][]string) map[int][]string {
	newMap := map[int][]string{}
	for gid, servers := range origin {
		copyServers := make([]string, len(servers))
		copy(copyServers, servers)
		newMap[gid] = copyServers
	}

	return newMap
}

func (sm *ShardMaster) executeQuery(op Op) (Err, Config) {
	last := sm.getLatestConfig()
	if op.CfgNum == -1 || op.CfgNum > last.Num {
		return OK, last
	}

	return OK, sm.configs[op.CfgNum]
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	var mutex sync.Mutex
	sm.mu = mutex

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.dead = 0
	sm.sessions = make(map[int64]Session)
	sm.notifyChan = make(map[int]chan Reply)

	go sm.applyConfigChange()
	return sm
}
