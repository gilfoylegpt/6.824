package raft

func (rf *Raft) SnapShot(snapshotIndex int, snapshotData []byte) {
	rf.mu.Lock()
	if snapshotIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	DPrintf("[SNAPSHOT INFO]: raft server %d active snapshot with lastIncludedIndex %d snapshotIndex %d\n", rf.me, rf.lastIncludedIndex, snapshotIndex)
	newlog := []LogEntry{{Term: rf.logEntries[snapshotIndex-rf.lastIncludedIndex].Term, Index: snapshotIndex}}
	rf.logEntries = append(newlog, rf.logEntries[snapshotIndex-rf.lastIncludedIndex+1:]...)
	rf.lastIncludedIndex = rf.logEntries[0].Index
	rf.lastIncludedTerm = rf.logEntries[0].Term
	rf.persist()
	state := rf.persister.ReadRaftState()
	rf.persister.SaveStateAndSnapshot(state, snapshotData)
	isLeader := rf.state == Leader
	rf.mu.Unlock()

	if isLeader {
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.leaderSendSnapshot(i, snapshotData)
		}
	}
}

func (rf *Raft) leaderSendSnapshot(idx int, snapshotData []byte) {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	if rf.killed() {
		return
	}

	if rf.checkOutdated(Leader, term) {
		return
	}

	rf.mu.Lock()
	args := InstallSnapShotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedTerm:  rf.lastIncludedTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		SnapshotData:      snapshotData,
	}
	rf.mu.Unlock()

	reply := InstallSnapShotReply{}
	ok := rf.sendSnapshot(idx, &args, &reply)
	if !ok {
		// DPrintf("[SNAPSHOT INFO]: raft server %d passive snapshot failed\n", idx)
		return
	} else {
		if rf.checkOutdated(Leader, term) {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.checkQuitFollower(reply.Term) {
			rf.resetTimer()
			go rf.handleTimeout()
			return
		}

		if reply.Accept {
			// DPrintf("[SNAPSHOT INFO]: raft server %d passive snapshot succcess lastIncludedIndex %d\n", idx, args.LastIncludedIndex)
			rf.matchIndex[idx] = max(rf.matchIndex[idx], args.LastIncludedIndex)
			rf.nextIndex[idx] = rf.matchIndex[idx] + 1
		}
	}
}

func (rf *Raft) CondSnapshot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || rf.activeSnapshotFlag || rf.passiveSnapshotFlag {
		reply.Term = rf.currentTerm
		reply.Accept = false
		if rf.activeSnapshotFlag {
			DPrintf("[SNAPSHOT WARNING]: raft server %d is going on active snapshot so won't do passive snapshot\n", rf.me)
		}
		if rf.passiveSnapshotFlag {
			DPrintf("[SNAPSHOT WARNING]: raft server %d is going on passive snapshot so won't do another passive snapshot before previous is done\n", rf.me)
		}
		return
	}

	wasLeader := rf.state == Leader
	rf.checkQuitFollower(args.Term)
	rf.state = Follower
	rf.resetTimer()
	if wasLeader {
		go rf.handleTimeout()
	}

	snapshotIndex := args.LastIncludedIndex
	snapshotTerm := args.LastIncludedTerm
	reply.Term = rf.currentTerm
	if snapshotIndex <= rf.lastIncludedIndex {
		reply.Accept = false
		return
	}

	DPrintf("[SNAPSHOT INFO]: raft server %d passive snapshot with lastAppliedIndex %d snapshotIndex %d\n", rf.me, rf.lastAppliedIndex, snapshotIndex)
	if rf.lastAppliedIndex > snapshotIndex {
		DPrintf("[SNAPSHOT WARNING]: raft server %d passive snapshot lastAppliedIndex %d > snapshotIndex %d and need to revert lastappliedindex to snapshotIndex\n",
			rf.me, rf.lastAppliedIndex, snapshotIndex)
	}

	rf.lastAppliedIndex = snapshotIndex
	if snapshotIndex >= rf.logEntries[len(rf.logEntries)-1].Index || rf.logEntries[snapshotIndex-rf.lastIncludedIndex].Term != snapshotTerm {
		rf.logEntries = []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
		rf.commitedIndex = snapshotIndex
	} else {
		newlog := []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
		rf.logEntries = append(newlog, rf.logEntries[snapshotIndex-rf.lastIncludedIndex+1:]...)
		rf.commitedIndex = max(rf.commitedIndex, snapshotIndex)
	}

	rf.lastIncludedIndex = snapshotIndex
	rf.lastIncludedTerm = snapshotTerm
	rf.persist()
	state := rf.persister.ReadRaftState()
	rf.passiveSnapshotFlag = true
	rf.persister.SaveStateAndSnapshot(state, args.SnapshotData)

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		SnapshotIndex: snapshotIndex,
		SnapShotData:  args.SnapshotData,
	}
	rf.applyCh <- applyMsg
	reply.Accept = true
	return
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedTerm  int
	LastIncludedIndex int
	SnapshotData      []byte
}

type InstallSnapShotReply struct {
	Term   int
	Accept bool
}

func (rf *Raft) sendSnapshot(idx int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[idx].Call("Raft.CondSnapshot", args, reply)
	return ok
}
