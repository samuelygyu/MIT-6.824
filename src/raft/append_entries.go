package raft
import (
	"math"
	"time"
	"6.5840/labrpc"
)

type RequestAppendEntriesArgs struct {
	// Leader's term
	Term int
	// So follower can redirect clients
	LeaderId int
	// PrevLogIndex
	PrevLogIndex int
	// PrevLogTerm
	PrevLogTerm int
	// Entries to store (empty for heartbeat; may send more than one for efficiency)
	Entries []Entry
	// LeaderCommit
	LeaderCommit int
}
type RequestAppendEntriesReply struct {
	// CurrentTerm, for candidate to update itself
	Term int
	// True if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
	// Current term, for leader to update itself
}

// Heartbeat RPC handler
func (rf *Raft) HandleAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// handle case which is not the leader
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// if the leader's log it's not the newest, return false
	argsLogLen := len(args.Entries)
	if argsLogLen != 0 {
		argsLastLog := args.Entries[len(args.Entries)-1]
		rfLastLog := rf.log[len(rf.log)-1]
		if argsLastLog.Term < rfLastLog.Term || (argsLastLog.Term == rfLastLog.Term && argsLastLog.Index < rfLastLog.Index) {
			reply.Success = false
			return
		}
	}

	// handle case which is the leader
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.resetRaft()
	rf.electionTime = time.Now()

	if len(rf.log) <= args.PrevLogIndex || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Success = false
		return
	}
	// if len(args.Entres) == 0, the follower's log is identical with the leader's log
	if argsLogLen != 0 {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
	}
	rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
	rf.signalL()
	reply.Success = true
}

func (rf *Raft) appendEntries() {
	for index, peer := range rf.peers {
		if index == rf.me {
			continue
		}
		// Retry to success
		// 2 case shutdown the loop
		// 1. currentTerm!= args.Term
		// 2. return success, that mean follower's log is consistent with leader's log
		go func(index int, peer *labrpc.ClientEnd) {
			for {
				rf.mu.Lock()
				args := RequestAppendEntriesArgs{
					Term:         rf.currentTerm,
					PrevLogIndex: rf.nextIndex[index] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[index]-1].Term,
					Entries:      rf.log[rf.nextIndex[index]:],
					LeaderCommit: rf.commitIndex,
				}
				reply := RequestAppendEntriesReply{}
				logLen := len(rf.log)
				rf.mu.Unlock()
				if peer.Call("Raft.HandleAppendEntries", &args, &reply) {
					rf.mu.Lock()
					// there is three case that reply.Success is false
					// 1. follower's term > currentTerm
					// 2. follower's log is newer than the leader's log
					// 3. the leader's nextIndex didn't match
					if !reply.Success {
						if reply.Term == 0 {
							// case 2
							rf.mu.Unlock()
							break
						} else if rf.currentTerm != reply.Term {
							// case 1
							rf.resetRaft()
							rf.currentTerm = reply.Term
							rf.mu.Unlock()
							break
						} else {
							// case 3
							rf.nextIndex[index] -= 1
							rf.mu.Unlock()
						}
					} else {
						// update the nextIndex and matchIndex
						rf.nextIndex[index] = logLen
						rf.matchIndex[index] = logLen - 1
						rf.mu.Unlock()
						break
					}
				} else {
					break
					// checkout if the requeset is fail, should be retry?
					// do not retry for now
				}
			}
		}(index, peer)
	}

	// caculate the commitIndex
	// 上面rpc是异步的，有可能执行commitIndex更新时，rpc没执行完全，导致replication数量不够，没能及时向状态机发送指令
	// the index of the log could be commit depend on the current log'term is similar to the leader's currentTerm
	start := rf.commitIndex + 1
	for i := start; i < len(rf.log); i++ {
		if rf.log[i].Term != rf.currentTerm {
			continue
		}
		n := 1
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= start {
				n++
			}
			if n > len(rf.peers)/2 {
				rf.commitIndex = i
			}
		}
	}

	// wait-up the applymsg thread
	rf.signalL()
}