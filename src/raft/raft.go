package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"mitds/labrpc"
	"sync"
	"time"
)

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	var mu sync.Mutex
	rf.mu = mu
	rf.dead = 0
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logEntries = []LogEntry{{Term: 0, Index: 0}}
	rf.commitedIndex = 0
	rf.lastAppliedIndex = 0
	rf.state = Follower
	rf.hbTime = 100 * time.Millisecond
	rf.timer = time.NewTimer(time.Duration(getRandMS(300, 500)) * time.Millisecond)
	rf.ready = false
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	DPrintf("[BOOT INFO]: raft server %d start\n", rf.me)

	go rf.handleTimeout()

	return rf
}

func (rf *Raft) handleTimeout() {
	for !rf.killed() {
		select {
		case <-rf.timer.C:
			state, _ := rf.status()
			switch state {
			case Follower:
				rf.resetTimer()
				go rf.runForElection()
			case Candidate:
				rf.resetTimer()
				rf.mu.Lock()
				if rf.ready {
					go rf.runForElection()
				} else {
					rf.ready = true
				}
				rf.mu.Unlock()
			case Leader:
				return
			}
		}
	}
}
