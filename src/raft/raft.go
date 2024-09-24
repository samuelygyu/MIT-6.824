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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Leader    Role = 1
	Candidate Role = 2
	Follower  Role = 3
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	condition sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	votedFor     int
	log          []Entry // 0 is the dummy note, start at 1
	state        Role
	electionTime time.Time

	commitIndex int
	lastApplied int

	// leader easy lost state
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

type Entry struct {
	Term    int
	Command interface{}
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func now() int64 {
	return time.Now().UnixNano() / 1e6
}

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

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == Leader
	if isLeader {
		rf.log = append(rf.log, Entry{
			Term:    rf.currentTerm,
			Command: command,
		})
		index = len(rf.log) - 1
		term = rf.currentTerm
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.appendEntries()
		}()
	}
	return index, term, isLeader
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

func (rf *Raft) ticker() {
	for {
		if rf.killed() {
			break
		}
		// for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader {
			rf.appendEntries()
		} else {
			// Time out
			if time.Since(rf.electionTime).Milliseconds() > 700 {
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.state = Candidate
				go rf.startElection(rf.currentTerm)
				rf.electionTime = time.Now()
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(time.Duration(50+(rand.Int63()%300)) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex && rf.lastApplied < len(rf.log){
			rf.lastApplied++
			msg := ApplyMsg {
				CommandValid: true,
				Command: rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else {
			rf.condition.Wait()
		}
	}	
}

func (rf *Raft) signalL() {
	rf.condition.Broadcast()
}

func (rf *Raft) startElection(currentTerm int) {
	rf.mu.Lock()
	voteGrantedCount := 1
	routeId := rand.Int() % 500
	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
		RouteId:      routeId,
	}

	var wg sync.WaitGroup
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()

			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			// if rf.sendRequestVote(peer, &args, &reply) {
			rf.mu.Lock()
			rf.mu.Unlock()
			if ok {
				rf.mu.Lock()
				if rf.currentTerm == args.Term && rf.state == Candidate {
					if reply.VoteGranted {
						voteGrantedCount++
						// Be leader
						if voteGrantedCount > len(rf.peers)/2 {
							rf.state = Leader
							rf.electionTime = time.Now()
							rf.votedFor = -1
							for i := range rf.nextIndex {
								rf.nextIndex[i] = len(rf.log)
							}
							rf.matchIndex = make([]int, len(rf.peers))
						}
					} else if reply.Term > rf.currentTerm {
						// UnProcess maybe is fine, cause the heartbeat handler wil change the state
						rf.currentTerm = reply.Term
						rf.resetRaft()
					} else {
						// Candidate's log term or index is not up to date
						// TODO need to checkout if the leader's log is not up to date with the only the one follower but the most follower
						// should it be switch to the follower
						// if true rf.resetRaft()
						// false do nothing
					}
				}
				rf.mu.Unlock()
			}
		}(peer)
	}
	rf.mu.Unlock()

	// wait for all the goroutine finish, if the cadidator compete was failed, reset the raft
	wg.Wait()
	if voteGrantedCount <= len(rf.peers)/2 {
		rf.mu.Lock()
		rf.resetRaft()
		rf.mu.Unlock()
	}
}

// reset the Raft to the orgin status
func (rf *Raft) resetRaft() {
	rf.votedFor = -1
	rf.state = Follower
	//TODO move to somewhere else rf.commitIndex = 0
	// rf.lastApplied = 0
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
	rf.condition = *sync.NewCond(&rf.mu)

	// Your initialization code here (3A, 3B, 3C).
	rf.resetRaft()
	rf.log = append(rf.log, Entry{})
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
