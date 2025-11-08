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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	index = rf.logEntries[len(rf.logEntries)-1].Index + 1
	if !isLeader {
		return index, term, isLeader
	}

	entry := LogEntry{
		Command: command,
		Index:   index,
		Term:    term,
	}
	DPrintf("[APPEND INFO]: client send command %v to leader %d\n", command, rf.me)
	rf.logEntries = append(rf.logEntries, entry)
	rf.persist()
	go rf.leaderAppendEntries()
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

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.passiveSnapshotFlag = false
	rf.activeSnapshotFlag = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.recoverFromSnapShot(rf.persister.ReadSnapshot())
	rf.persist()
	DPrintf("[BOOT INFO]: raft server %d start\n", rf.me)

	go rf.handleTimeout()
	go rf.applier()

	return rf
}

func (rf *Raft) recoverFromSnapShot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	DPrintf("[SNAPSHOT INFO]: raft server %d load snapshot from index %d\n", rf.me, rf.lastIncludedIndex)
	rf.lastAppliedIndex = rf.lastIncludedIndex
	rf.commitedIndex = rf.lastIncludedIndex
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		SnapshotIndex: rf.lastIncludedIndex,
		SnapShotData:  data,
	}
	go func(msg ApplyMsg) {
		rf.applyCh <- msg
	}(applyMsg)
}

// func (rf *Raft) applier() {
// 	for !rf.killed() {
// 		rf.mu.Lock()
// 		msgs := []ApplyMsg{}
// 		for rf.lastAppliedIndex < rf.commitedIndex {
// 			rf.lastAppliedIndex++
// 			msg := ApplyMsg{
// 				CommandValid: true,
// 				Command:      rf.logEntries[rf.lastAppliedIndex-rf.lastIncludedIndex].Command,
// 				CommandIndex: rf.logEntries[rf.lastAppliedIndex-rf.lastIncludedIndex].Index,
// 				CommandTerm:  rf.logEntries[rf.lastAppliedIndex-rf.lastIncludedIndex].Term,
// 			}
// 			msgs = append(msgs, msg)
// 		}
// 		rf.mu.Unlock()
// 		if len(msgs) > 0 {
// 			for _, msg := range msgs {
// 				rf.applyCh <- msg
// 			}
// 		}
// 		time.Sleep(rf.hbTime * 2)
// 	}
// }

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		var applyMsg ApplyMsg
		needApply := false

		if rf.lastAppliedIndex < rf.commitedIndex {
			rf.lastAppliedIndex++
			if rf.lastAppliedIndex <= rf.lastIncludedIndex {
				rf.lastAppliedIndex = rf.lastIncludedIndex
				rf.mu.Unlock()
				continue
			}

			applyMsg = ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				Command:       rf.logEntries[rf.lastAppliedIndex-rf.lastIncludedIndex].Command,
				CommandIndex:  rf.lastAppliedIndex,
				CommandTerm:   rf.logEntries[rf.lastAppliedIndex-rf.lastIncludedIndex].Term,
			}
			needApply = true
		}
		rf.mu.Unlock()

		if needApply {
			rf.applyCh <- applyMsg
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) CheckRebootRecovery() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitedIndex > 0 && rf.lastAppliedIndex == rf.commitedIndex
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
