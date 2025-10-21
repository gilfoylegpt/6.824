package raft

import (
	"sort"
)

func (rf *Raft) leaderAppendEntries() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.matchIndex[rf.me] = rf.logEntries[len(rf.logEntries)-1].Index
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(ii int) {
			if rf.killed() {
				return
			}

			if rf.checkOutdated(Leader, term) {
				return
			}

			rf.mu.Lock()
			entries := []LogEntry{}
			nextIdx := rf.nextIndex[ii]
			if nextIdx <= rf.lastIncludedIndex {
				go rf.leaderSendSnapshot(ii, rf.persister.ReadSnapshot())
				rf.mu.Unlock()
				return
			}
			if nextIdx <= rf.logEntries[len(rf.logEntries)-1].Index {
				entries = make([]LogEntry, len(rf.logEntries)+rf.lastIncludedIndex-nextIdx)
				copy(entries, rf.logEntries[nextIdx-rf.lastIncludedIndex:])
			}
			defer func() {
				if r := recover(); r != nil {
					DPrintf("[PANIC !!!]: Leader %d append server %d (Term:%d, nextIdx %d, lastIncludedIndex %d, len(logs) %d, entries %v\n",
						rf.me, ii, term, nextIdx, rf.lastIncludedIndex, len(rf.logEntries), rf.logEntries)
					panic(r)
				}
			}()
			prelog := rf.logEntries[nextIdx-rf.lastIncludedIndex-1]
			args := &AppendEntriesArgs{
				Term:              term,
				LeaderId:          rf.me,
				PreLogIndex:       prelog.Index,
				PreLogTerm:        prelog.Term,
				LogEntries:        entries,
				LeaderCommitIndex: rf.commitedIndex,
			}
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}
			DPrintf("[APPEND INFO]: Leader %d append server %d (Term:%d PreLogIndex:%d EntriesLen:%d CommitIndex:%d)\n",
				rf.me, ii, term, args.PreLogIndex, len(args.LogEntries), args.LeaderCommitIndex)
			ok := rf.sendAppendEntries(ii, args, reply)
			if !ok {
				//DPrintf("[RPC FAILED]: Leader %d Append Entries to server %d Failed\n", rf.me, ii)
				return
			}

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

			if reply.Success == false {
				possibleNextIdx := 0
				if reply.ConflictTerm == -1 {
					possibleNextIdx = reply.ConflictIndex
				} else {
					foundTerm := false
					j := len(rf.logEntries) - 1
					for ; j > 0; j-- {
						if rf.logEntries[j].Term == reply.ConflictTerm {
							foundTerm = true
							break
						}
					}

					if foundTerm {
						possibleNextIdx = rf.logEntries[j+1].Index
					} else {
						possibleNextIdx = reply.ConflictIndex
					}
				}

				if possibleNextIdx < rf.nextIndex[ii] && possibleNextIdx > rf.matchIndex[ii] {
					rf.nextIndex[ii] = possibleNextIdx
				} else {
					return
				}
			} else {
				possibleMatchIdx := args.PreLogIndex + len(args.LogEntries)
				rf.matchIndex[ii] = max(possibleMatchIdx, rf.matchIndex[ii])
				rf.nextIndex[ii] = rf.matchIndex[ii] + 1
				DPrintf("[NEXT INDEX]: term %d nextIndex[server %d] = %d\n", rf.currentTerm, ii, rf.nextIndex[ii])
				sortMatchIndex := make([]int, len(rf.matchIndex))
				copy(sortMatchIndex, rf.matchIndex)
				sort.Ints(sortMatchIndex)
				j := sortMatchIndex[(len(sortMatchIndex)-1)/2]
				for k := j; k > rf.commitedIndex; k-- {
					if rf.logEntries[k-rf.lastIncludedIndex].Term == rf.currentTerm {
						rf.commitedIndex = k
						DPrintf("[APPEND INFO]: Leader %d committed to index %d\n", rf.me, rf.commitedIndex)
						break
					}
				}
			}
		}(i)
	}
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PreLogIndex       int
	PreLogTerm        int
	LogEntries        []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	wasLeader := rf.state == Leader
	rf.checkQuitFollower(args.Term)
	rf.state = Follower
	rf.resetTimer()
	if wasLeader {
		go rf.handleTimeout()
	}

	if args.PreLogIndex < rf.lastIncludedIndex {
		if len(args.LogEntries) == 0 || args.LogEntries[len(args.LogEntries)-1].Index <= rf.lastIncludedIndex {
			reply.Term = rf.currentTerm
			reply.Success = true
			return
		} else {
			args.LogEntries = args.LogEntries[rf.lastIncludedIndex-args.PreLogIndex:]
			args.PreLogIndex = rf.lastIncludedIndex
			args.PreLogTerm = rf.lastIncludedTerm
		}
	}

	// DPrintf("[APPEND INFO]: raft server %d got append from leader %d (Term:%d EntriesLen:%d)\n", rf.me, args.LeaderId, args.Term, len(args.LogEntries))
	if args.PreLogIndex > rf.logEntries[len(rf.logEntries)-1].Index || rf.logEntries[args.PreLogIndex-rf.lastIncludedIndex].Term != args.PreLogTerm {
		if args.PreLogIndex > rf.logEntries[len(rf.logEntries)-1].Index {
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.logEntries[len(rf.logEntries)-1].Index + 1
		} else {
			reply.ConflictTerm = rf.logEntries[args.PreLogIndex-rf.lastIncludedIndex].Term
			i := args.PreLogIndex - 1 - rf.lastIncludedIndex
			for i >= 0 && rf.logEntries[i].Term == reply.ConflictTerm {
				i--
			}
			reply.ConflictIndex = i + 1 + rf.lastIncludedIndex
		}
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else {
		misMatchIdx := -1
		for i, entry := range args.LogEntries {
			if args.PreLogIndex+1+i > rf.logEntries[len(rf.logEntries)-1].Index || rf.logEntries[args.PreLogIndex+1+i-rf.lastIncludedIndex].Term != entry.Term {
				misMatchIdx = args.PreLogIndex + 1 + i
				break
			}
		}

		if misMatchIdx != -1 {
			newlog := rf.logEntries[:misMatchIdx-rf.lastIncludedIndex]
			newlog = append(newlog, args.LogEntries[misMatchIdx-args.PreLogIndex-1:]...)
			rf.logEntries = newlog
		}

		rf.persist()

		if args.LeaderCommitIndex > rf.commitedIndex {
			rf.commitedIndex = min(args.LeaderCommitIndex, rf.logEntries[len(rf.logEntries)-1].Index)
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
