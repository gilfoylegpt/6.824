package raft

import (
	"sync"
	"time"
)

func (rf *Raft) runForElection() {
	term := rf.convert2Candidate()

	votes := 1
	finished := 1
	var voteMutex sync.Mutex
	cond := sync.NewCond(&voteMutex)

	for i, _ := range rf.peers {
		if rf.killed() {
			return
		}

		if rf.checkOutdated(Candidate, term) {
			return
		}

		if i == rf.me {
			continue
		}

		go func(ii int) {
			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:              term,
				CandidateId:       rf.me,
				LastLogEntryTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
				LastLogEntryIndex: rf.logEntries[len(rf.logEntries)-1].Index,
			}
			rf.mu.Unlock()
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(ii, args, reply)
			if !ok {
				DPrintf("[RPC FAILED]: candidate %d request vote from %d failed\n", rf.me, ii)
				return
			}

			if rf.checkOutdated(Candidate, term) {
				return
			}

			rf.mu.Lock()
			if rf.checkQuitFollower(reply.Term) {
				rf.mu.Unlock()
				rf.resetTimer()
				return
			}
			rf.mu.Unlock()

			voteMutex.Lock()
			if reply.VoteGranted {
				DPrintf("[ELECTION INFO]: candidate %d got vote from %d\n", rf.me, ii)
				votes++
			}
			finished++
			voteMutex.Unlock()
			cond.Broadcast()
		}(i)
	}

	totalNum := len(rf.peers)
	majorityNum := totalNum/2 + 1
	voteMutex.Lock()
	for votes < majorityNum && finished != totalNum {
		cond.Wait()
		if rf.checkOutdated(Candidate, term) {
			voteMutex.Unlock()
			return
		}
	}

	if votes >= majorityNum {
		rf.convert2Leader()
	} else {
		DPrintf("[ELECTION INFO]: candidate %d failed in the election with term %d\n", rf.me, term)
	}
	voteMutex.Unlock()
}

func (rf *Raft) convert2Leader() {
	rf.mu.Lock()
	rf.state = Leader
	DPrintf("[ELECTION INFO]: candidate %d became leader with term %d\n", rf.me, rf.currentTerm)
	term := rf.currentTerm
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.logEntries[len(rf.logEntries)-1].Index + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	go func() {
		for !rf.killed() {
			if rf.checkOutdated(Leader, term) {
				return
			}
			go rf.leaderAppendEntries()
			time.Sleep(rf.hbTime)
		}
	}()
}

func (rf *Raft) convert2Candidate() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.persist()
	DPrintf("[ELECTION INFO]: rafer server %d became candidate, currentTerm %d\n", rf.me, rf.currentTerm)
	rf.ready = false
	return rf.currentTerm
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term              int
	CandidateId       int
	LastLogEntryIndex int
	LastLogEntryTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	state := rf.state
	flag := rf.checkQuitFollower(args.Term)
	// when candidate or leader quit into follower
	// initialize to follower
	if state != Follower && flag {
		rf.resetTimer()
		if state == Leader {
			go rf.handleTimeout()
		}
	}

	uptodate := false
	lastLog := rf.logEntries[len(rf.logEntries)-1]
	if (args.LastLogEntryTerm > lastLog.Term) || (args.LastLogEntryTerm == lastLog.Term && args.LastLogEntryIndex >= lastLog.Index) {
		uptodate = true
	}

	// For follower, if it vote for other candidate, it should reset its timer
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && uptodate {
		rf.voteFor = args.CandidateId
		rf.persist()
		rf.resetTimer()
		reply.VoteGranted = true
	} else {
		// for follower not vote or candidate or leader
		// just leave timer alone
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
