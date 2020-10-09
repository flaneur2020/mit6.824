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
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RaftState int

const (
	RaftStateFollower  RaftState = 0
	RaftStateCandidate RaftState = 1
	RaftStateLeader    RaftState = 2
)

func (s RaftState) String() string {
	switch s {
	case RaftStateFollower:
		return "FOLLOWER"
	case RaftStateCandidate:
		return "CANDIDATE"
	case RaftStateLeader:
		return "LEADER"
	default:
		return "<invalid>"
	}
}

// default ticks for timeouts
const (
	defaultHeartBeatTimeoutTicks uint = 1000
	defaultElectionTimeoutTicks  uint = 100
	defaultTickIntervalMs             = 100
)

// raft event
type raftEV struct {
	args  interface{}
	c     chan struct{}
	reply interface{}
}

func newRaftEV(args interface{}) *raftEV {
	return &raftEV{
		c:     make(chan struct{}, 1),
		args:  args,
		reply: nil,
	}
}

func (ev *raftEV) Done(reply interface{}) {
	ev.reply = reply
	close(ev.c)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state    RaftState
	eventc   chan *raftEV
	quitc    chan struct{}
	quitOnce sync.Once

	// volatile state on leaders
	nextIndex  []uint
	matchIndex []uint

	// volatile state on all servers
	commitIndex uint

	// persistent state on all servers
	term        int
	votedFor    int
	lastApplied uint

	// clock in ticks
	heartbeatTimeoutTicks uint
	electionTimeoutTicks  uint
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.term, rf.state == RaftStateLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         uint
	CandidateID  uint
	LastLogIndex uint
	LastLogTerm  uint
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        uint
	VoteGranted bool
}

// TODO
type AppendEntriesArgs struct {
	Term         uint
	LeaderID     uint
	CommitIndex  uint
	PrevLogIndex uint
	PrevLogTerm  uint

	// TODO: logEntries
}

// TODO
type AppendEntriesReply struct {
	Term         uint
	PeerId       uint
	Success      bool
	LastLogIndex uint
}

type TickEventArgs struct{}
type TickEventReply struct{}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	ev := newRaftEV(args)
	rf.eventc <- ev
	<-ev.c
	*reply = *(ev.reply.(*RequestVoteReply))
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// send appendEntries RPC to remote server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.quitOnce.Do(func() {
		close(rf.quitc)
	})
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = RaftStateLeader
	rf.nextIndex = make([]uint, len(peers))
	rf.matchIndex = make([]uint, len(peers))

	rf.heartbeatTimeoutTicks = defaultHeartBeatTimeoutTicks
	rf.electionTimeoutTicks = defaultElectionTimeoutTicks

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.eventc = make(chan *raftEV, 1)
	rf.quitc = make(chan struct{})
	go rf.loopEV()
	go rf.loopTicks()

	return rf
}

func (rf *Raft) loopEV() {
	for {
		select {
		case ev := <-rf.eventc:
			switch rf.state {
			case RaftStateLeader:
				rf.stepLeader(ev)
			case RaftStateCandidate:
				rf.stepCandidate(ev)
			case RaftStateFollower:
				rf.stepFollower(ev)
			}

		case <-rf.quitc:
			break
		}
	}
}

func (rf *Raft) loopTicks() {
	for {
		select {
		case <-rf.quitc:
			break
		}

		time.Sleep(defaultTickIntervalMs * time.Millisecond)
		rf.eventc <- newRaftEV(&TickEventArgs{})
	}
}

func (rf *Raft) stepRaft(ev *raftEV) {
}

func (rf *Raft) stepLeader(ev *raftEV) {
	// TODO
}

func (rf *Raft) stepFollower(ev *raftEV) {
	switch args := ev.args.(type) {
	case *TickEventArgs:
		rf.electionTimeoutTicks--
		// on election timeout
		if rf.electionTimeoutTicks <= 0 {
			rf.become(RaftStateCandidate)
			// TODO: broadcast requestVote rpcs
			return
		}
		ev.Done(&TickEventReply{})

	case *AppendEntriesArgs:
		rf.resetElectionTimeoutTicks()
		reply := rf.processAppendEntries(args)
		ev.Done(reply)

	case *RequestVoteArgs:
		reply := rf.processRequestVote(args)
		ev.Done(reply)
	}
}

func (rf *Raft) stepCandidate(ev *raftEV) {
	// TODO
}

func (rf *Raft) become(state RaftState) {
	rf.state = state
	if state == RaftStateFollower {
		rf.resetElectionTimeoutTicks()
	} else if state == RaftStateCandidate {
		rf.resetElectionTimeoutTicks()
	} else if state == RaftStateLeader {
		rf.heartbeatTimeoutTicks = defaultHeartBeatTimeoutTicks
		// TODO: initialize nextIndex and matchIndex
	}
}

func (rf *Raft) resetElectionTimeoutTicks() {
	// TODO: rand it
	rf.electionTimeoutTicks = defaultElectionTimeoutTicks
}

func (rf *Raft) processAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	return nil
}

func (rf *Raft) processRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	return nil
}
