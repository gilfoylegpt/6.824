package kvraft

import (
	"bytes"
	"log"
	"mitds/labgob"
	"mitds/labrpc"
	"mitds/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const RespondTimeOut = 500

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key        string
	Value      string
	OpType     string
	ClientId   int64
	CommandNum int
}

type Session struct {
	LastCommandNum int
	OpType         string
	Response       Reply
}

type Reply struct {
	Value string
	Err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdb                  map[string]string
	sessions              map[int64]Session
	notifyChannel         map[int]chan Reply
	lastAppliedOpIndex    int
	passiveSnapshotBefore bool
}

func (kv *KVServer) createChannel(index int) chan Reply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.notifyChannel[index] = make(chan Reply, 1)
	return kv.notifyChannel[index]
}

func (kv *KVServer) destroyChannel(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.notifyChannel[index]; ok {
		close(kv.notifyChannel[index])
		delete(kv.notifyChannel, index)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:        args.Key,
		OpType:     "Get",
		ClientId:   args.ClientId,
		CommandNum: args.CommandNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.createChannel(index)
	select {
	case r := <-ch:
		reply.Value = r.Value
		reply.Err = r.Err
	case <-time.After(RespondTimeOut * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	kv.destroyChannel(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:        args.Key,
		Value:      args.Value,
		OpType:     args.Op,
		ClientId:   args.ClientId,
		CommandNum: args.CommandNum,
	}

	kv.mu.Lock()
	if session, ok := kv.sessions[args.ClientId]; ok && args.CommandNum <= session.LastCommandNum {
		reply.Err = session.Response.Err
		kv.mu.Unlock()
		return
	} else {
		kv.mu.Unlock()
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		ch := kv.createChannel(index)
		select {
		case r := <-ch:
			reply.Err = r.Err
		case <-time.After(RespondTimeOut * time.Millisecond):
			reply.Err = ErrTimeOut
		}
		kv.destroyChannel(index)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyMessage() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			kv.mu.Lock()
			if applyMsg.CommandIndex <= kv.lastAppliedOpIndex {
				kv.mu.Unlock()
				continue
			}

			if kv.passiveSnapshotBefore {
				if applyMsg.CommandIndex != kv.lastAppliedOpIndex+1 {
					kv.mu.Unlock()
					continue
				}
				kv.passiveSnapshotBefore = false
			}

			kv.lastAppliedOpIndex = applyMsg.CommandIndex
			op, ok := applyMsg.Command.(Op)
			if ok {
				var reply Reply
				if session, ok := kv.sessions[op.ClientId]; ok && op.OpType != "Get" && op.CommandNum <= session.LastCommandNum {
					reply = session.Response
				} else {
					switch op.OpType {
					case "Get":
						if v, ok := kv.kvdb[op.Key]; ok {
							reply.Err = OK
							reply.Value = v
						} else {
							reply.Err = ErrNoKey
							reply.Value = ""
						}
					case "Put":
						kv.kvdb[op.Key] = op.Value
						reply.Err = OK
					case "Append":
						if v, ok := kv.kvdb[op.Key]; ok {
							kv.kvdb[op.Key] = v + op.Value
							reply.Err = OK
						} else {
							kv.kvdb[op.Key] = op.Value
							reply.Err = ErrNoKey
						}
					}

					if op.OpType != "Get" {
						kv.sessions[op.ClientId] = Session{
							LastCommandNum: op.CommandNum,
							OpType:         op.OpType,
							Response:       reply,
						}
					}
				}

				if ch, ok := kv.notifyChannel[applyMsg.CommandIndex]; ok {
					if term, isLeader := kv.rf.GetState(); isLeader && term == applyMsg.CommandTerm {
						ch <- reply
					}
				}
			}
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			kv.applySnapshotToSM(applyMsg.SnapShotData)
			kv.lastAppliedOpIndex = applyMsg.SnapshotIndex
			kv.passiveSnapshotBefore = true
			kv.mu.Unlock()
			kv.rf.SetPassiveSnapshotFlag(false)
		}
	}
}

func (kv *KVServer) applySnapshotToSM(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)
	var kvdb map[string]string
	var sessions map[int64]Session
	if d.Decode(&kvdb) != nil || d.Decode(&sessions) != nil {
		DPrintf("applySnapshotToSM failed!")
	} else {
		kv.kvdb = kvdb
		kv.sessions = sessions
	}
}

func (kv *KVServer) checkSnapshotNeed() {
	for !kv.killed() {

		if kv.rf.GetPassiveAndSetActiveFlag() {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		var snapshotIndex int 
		var snapshotData []byte 
		if kv.maxraftstate != -1 && float64(kv.rf.GetRaftStateSize()) / float64(kv.maxraftstate) > 0.9 {
			kv.mu.Lock()
			snapshotIndex = kv.lastAppliedOpIndex 
			b := new(bytes.Buffer)
			e := labgob.NewEncoder(b)
			e.Encode(kv.kvdb)
			e.Encode(kv.sessions)
			snapshotData = b.Bytes() 
			kv.mu.Unlock()
		}

		if snapshotData != nil {
			kv.rf.SnapShot(snapshotIndex, snapshotData)
		}

		kv.rf.SetActiveSnapshotFlag(false)
		time.Sleep(50 * time.Microsecond)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.dead = 0

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvdb = make(map[string]string)
	kv.sessions = make(map[int64]Session)
	kv.notifyChannel = make(map[int]chan Reply)
	kv.lastAppliedOpIndex = 0
	kv.passiveSnapshotBefore = false

	go kv.applyMessage()
	go kv.checkSnapshotNeed()

	return kv
}
