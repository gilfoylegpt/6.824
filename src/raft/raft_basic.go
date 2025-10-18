package raft

import (
	"bytes"
	"mitds/labgob"
	"mitds/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	SnapshotIndex int
	SnapShotData  []byte
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	logEntries  []LogEntry

	commitedIndex    int
	lastAppliedIndex int

	nextIndex  []int
	matchIndex []int

	state  ServerState
	hbTime time.Duration
	timer  *time.Timer
	ready  bool

	applyCh chan ApplyMsg

	lastIncludedTerm  int
	lastIncludedIndex int

	activeSnapshotFlag  bool
	passiveSnapshotFlag bool
}

func (rf *Raft) SetPassiveSnapshotFlag (flag bool ) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.passiveSnapshotFlag = flag 
}

func (rf *Raft) SetActiveSnapshotFlag (flag bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.activeSnapshotFlag = flag
}

func (rf *Raft) GetPassiveAndSetActiveFlag () bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.passiveSnapshotFlag {
		rf.activeSnapshotFlag = true 
	}
	return rf.passiveSnapshotFlag 
}

func (rf *Raft) GetRaftStateSize () int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logEntries []LogEntry
	var lastIncludedTerm int 
	var lastIncludedIndex int 
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&lastIncludedTerm) != nil || 
		d.Decode(&lastIncludedIndex) != nil {
		DPrintf("[PERSIST ERROR]: raft server %d readPersist error\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logEntries = logEntries
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) status() (ServerState, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state, rf.currentTerm
}

func (rf *Raft) resetTimer() {
	rf.timer.Stop()
	rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)
}

func (rf *Raft) checkOutdated(state ServerState, term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (state != -1 && state != rf.state) || (term != -1 && term != rf.currentTerm) {
		return true
	}

	return false
}

func (rf *Raft) checkQuitFollower(term int) bool {
	if rf.currentTerm < term {
		rf.state = Follower
		rf.currentTerm = term
		rf.voteFor = -1
		rf.persist()
		return true
	}
	return false
}
