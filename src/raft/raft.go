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
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
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
	RaftFollower  RaftState = 0
	RaftCandidate RaftState = 1
	RaftLeader    RaftState = 2
)

func (s RaftState) String() string {
	switch s {
	case RaftFollower:
		return "FOLLOWER"
	case RaftCandidate:
		return "CANDIDATE"
	case RaftLeader:
		return "LEADER"
	default:
		return "<invalid>"
	}
}

// default ticks for timeouts
const (
	defaultHeartBeatTimeoutTicks uint = 1
	defaultElectionTimeoutTicks  uint = 10
	defaultTickIntervalMs             = 100
)

// raft event
type raftEV struct {
	args   interface{}
	c      chan struct{}
	result interface{} // maybe nil
}

func newRaftEV(args interface{}) *raftEV {
	return &raftEV{
		c:      make(chan struct{}, 1),
		args:   args,
		result: nil,
	}
}

func (ev *raftEV) Done(result interface{}) {
	ev.result = result
	close(ev.c)
}

// raft log entry
type raftLogEntry struct {
	Index   int
	Term    int
	Command interface{}
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
	applyCh  chan ApplyMsg

	logEntries []raftLogEntry

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// volatile state on all servers
	commitIndex int
	applyIndex  int

	// volatile state on candidates
	voteGranted map[int]bool

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
	return rf.term, rf.state == RaftLeader
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
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	PeerID      int
	Message     string
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	CommitIndex  int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []raftLogEntry
}

type AppendEntriesReply struct {
	Term         int
	PeerID       int
	Success      bool
	LastLogIndex int
	Message      string
}

type TickEventArgs struct{}

type DispatchCommandArgs struct {
	Command interface{}
}

type DispatchCommandReply struct {
	IsLeader bool
	Index    int
	Term     int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	ev := newRaftEV(args)
	rf.eventc <- ev
	<-ev.c
	if ev.result == nil {
		panic("unexpected nil result")
	}
	*reply = *(ev.result.(*RequestVoteReply))
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ev := newRaftEV(args)
	rf.eventc <- ev
	<-ev.c
	if ev.result == nil {
		panic("unexpected nil result")
	}
	*reply = *(ev.result.(*AppendEntriesReply))
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
	ev := newRaftEV(&DispatchCommandArgs{
		Command: command,
	})

	rf.eventc <- ev
	<-ev.c

	if ev.result == nil {
		return -1, -1, false
	}

	reply := ev.result.(*DispatchCommandReply)
	return reply.Index, reply.Term, reply.IsLeader
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
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.voteGranted = map[int]bool{}
	rf.commitIndex = -1
	rf.applyIndex = -1

	rf.heartbeatTimeoutTicks = defaultHeartBeatTimeoutTicks
	rf.becomeFollower()

	rf.votedFor = -1
	rf.term = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCh = applyCh
	rf.eventc = make(chan *raftEV, 1)
	rf.quitc = make(chan struct{})
	go rf.loopEV()
	go rf.loopTicks()

	log.Printf("%v raft.start", rf.logPrefix())
	return rf
}

func (rf *Raft) loopEV() {
	for {
		select {
		case ev := <-rf.eventc:
			switch rf.state {
			case RaftLeader:
				rf.stepLeader(ev)

			case RaftCandidate:
				rf.stepCandidate(ev)

			case RaftFollower:
				rf.stepFollower(ev)
			}

		case <-rf.quitc:
			break
		}
	}
}

func (rf *Raft) loopTicks() {
	for {
		time.Sleep(defaultTickIntervalMs * time.Millisecond)
		select {
		case <-rf.quitc:
			break
		case rf.eventc <- newRaftEV(&TickEventArgs{}):
		}
	}
}

func (rf *Raft) stepLeader(ev *raftEV) {
	switch v := ev.args.(type) {
	case *TickEventArgs:
		rf.heartbeatTimeoutTicks--
		if rf.heartbeatTimeoutTicks <= 0 {
			rf.heartbeatTimeoutTicks = defaultHeartBeatTimeoutTicks
			rf.broadcastAppendEntries()
		}
		ev.Done(nil)

	case *AppendEntriesReply:
		rf.processAppendEntriesReply(v)
		ev.Done(nil)

	case *AppendEntriesArgs:
		reply := rf.processAppendEntries(v)
		ev.Done(reply)

	case *RequestVoteArgs:
		reply := rf.processRequestVote(v)
		ev.Done(reply)

	case *DispatchCommandArgs:
		reply := rf.processDispatchCommand(v)
		ev.Done(reply)

	default:
		log.Printf("%v step-leader.unexpected-ev %#v", rf.logPrefix(), v)
		ev.Done(nil)
	}
}

func (rf *Raft) stepFollower(ev *raftEV) {
	switch v := ev.args.(type) {
	case *TickEventArgs:
		// log.Printf("follower.tick me=%v", rf.me)
		rf.electionTimeoutTicks--
		// on election timeout
		if rf.electionTimeoutTicks <= 0 {
			log.Printf("%v step-follower.election-timeout start election", rf.logPrefix())
			rf.startElection()
			return
		}
		ev.Done(nil)

	case *AppendEntriesArgs:
		reply := rf.processAppendEntries(v)
		ev.Done(reply)

	case *RequestVoteArgs:
		reply := rf.processRequestVote(v)
		ev.Done(reply)

	default:
		log.Printf("%v step-follower.unexpected-ev %#v", rf.logPrefix(), v)
		ev.Done(nil)
	}
}

func (rf *Raft) stepCandidate(ev *raftEV) {
	switch v := ev.args.(type) {
	case *TickEventArgs:
		rf.electionTimeoutTicks--
		// on election timeout
		if rf.electionTimeoutTicks <= 0 {
			rf.startElection()
			return
		}
		ev.Done(nil)

	case *RequestVoteReply:
		if v.VoteGranted {
			rf.voteGranted[v.PeerID] = true
		}
		expectedVotes := int(math.Floor(float64(len(rf.peers))/2) + 1)
		if len(rf.voteGranted)+1 >= expectedVotes {
			log.Printf("%v step-candidate.im-leader! votes-from=%v expectedVotes=%v", rf.logPrefix(), rf.voteGranted, expectedVotes)
			rf.becomeLeader()
		}
		ev.Done(nil)

	case *RequestVoteArgs:
		reply := rf.processRequestVote(v)
		ev.Done(reply)

	case *AppendEntriesArgs:
		reply := rf.processAppendEntries(v)
		ev.Done(reply)

	default:
		log.Printf("%v step-candidate.unexpected-ev %#v", rf.logPrefix(), v)
		ev.Done(nil)
	}
}

func (rf *Raft) becomeFollower() {
	log.Printf("%v become-follower", rf.logPrefix())
	rf.state = RaftFollower
	rf.resetElectionTimeoutTicks()
}

func (rf *Raft) becomeLeader() {
	log.Printf("%v become-leader", rf.logPrefix())
	rf.state = RaftLeader
	rf.heartbeatTimeoutTicks = defaultHeartBeatTimeoutTicks

	lastIndex, _ := rf.lastLogInfo()

	for idx := range rf.matchIndex {
		rf.matchIndex[idx] = -1
		rf.nextIndex[idx] = lastIndex + 1
	}

	// append one nop log entry after a new leader got elected
	rf.processDispatchCommand(&DispatchCommandArgs{
		Command: "nop",
	})
}

// On conversion to candidate, start election:
// - Increment currentTerm
// - Vote for self
// - Reset election timer
// - Send RequestVote RPCs to all other servers
func (rf *Raft) startElection() {
	log.Printf("%v become-candidate.start-election", rf.logPrefix())
	rf.state = RaftCandidate
	rf.resetElectionTimeoutTicks()

	rf.term++
	rf.votedFor = rf.me
	rf.persist()

	rf.voteGranted = map[int]bool{}
	rf.broadcastRequestVote()
}

// lastLogInfo returns the last log index and term
func (rf *Raft) lastLogInfo() (int, int) {
	if len(rf.logEntries) == 0 {
		return -1, -1
	}
	logEntry := rf.logEntries[len(rf.logEntries)-1]
	return logEntry.Index, logEntry.Term
}

// returns the log entries begins with logIndex
func (rf *Raft) logEntriesSince(logIndex int) []raftLogEntry {
	if logIndex < 0 {
		logIndex = 0
	}
	return rf.logEntries[logIndex:]
}

func (rf *Raft) appendLogEntriesSince(logIndex int, entries []raftLogEntry) {
	if logIndex < 0 {
		logIndex = 0
	}

	rf.logEntries = rf.logEntries[0:logIndex]
	rf.logEntries = append(rf.logEntries, entries...)
	for i := logIndex; i < len(rf.logEntries); i++ {
		if rf.logEntries[i].Index != i {
			panic(fmt.Sprintf("bad logIndex, log.Index: %d, i: %d", rf.logEntries[i].Index, i))
		}
	}
}

func (rf *Raft) appendLogEntryByCommand(command interface{}, term int) {
	lastIndex, _ := rf.lastLogInfo()
	logEntry := raftLogEntry{
		Index:   lastIndex + 1,
		Term:    term,
		Command: command,
	}
	rf.logEntries = append(rf.logEntries, logEntry)
}

func (rf *Raft) applyLogs() {
	for i := rf.applyIndex + 1; i <= rf.commitIndex; i++ {
		entry := rf.logEntries[i]

		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}

		log.Printf("%v apply-logs msg=%#v", rf.logPrefix(), &msg)
		rf.applyCh <- msg
		rf.applyIndex = i
	}
}

func (rf *Raft) resetElectionTimeoutTicks() {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	delta := uint(rnd.Int63n(int64(defaultElectionTimeoutTicks)))
	rf.electionTimeoutTicks = defaultElectionTimeoutTicks + delta
}

func (rf *Raft) processDispatchCommand(args *DispatchCommandArgs) *DispatchCommandReply {
	if rf.state != RaftLeader {
		return &DispatchCommandReply{IsLeader: false, Index: -1, Term: -1}
	}

	log.Printf("%v process-dispatch-command cmd=%#v", rf.logPrefix(), args.Command)

	rf.appendLogEntryByCommand(args.Command, rf.term)
	lastIndex, lastTerm := rf.lastLogInfo()

	rf.applyLogs()

	return &DispatchCommandReply{
		IsLeader: true,
		Index:    lastIndex,
		Term:     lastTerm,
	}
}

func (rf *Raft) processAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	lastIndex, _ := rf.lastLogInfo()

	reply := &AppendEntriesReply{
		Success:      false,
		Term:         rf.term,
		PeerID:       rf.me,
		LastLogIndex: lastIndex,
		Message:      "",
	}

	defer func() {
		log.Printf("%v process-append-entries from-leader=%v entries=%d reply=%v", rf.logPrefix(), args.LeaderID, len(args.LogEntries), reply.Message)
	}()

	if args.Term < rf.term {
		reply.Message = "args.Term < rf.term"
		return reply
	} else if args.Term == rf.term {
		if rf.state == RaftLeader {
			reply.Message = "I'm leader"
			return reply
		} else if rf.state == RaftCandidate {
			rf.becomeFollower()
		}
	} else if args.Term > rf.term {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		rf.becomeFollower()
		rf.term = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	if args.PrevLogIndex > lastIndex {
		reply.Message = fmt.Sprintf("args.prevLogIndex(%v) > lastIndex(%v)", args.PrevLogIndex, lastIndex)
		return reply
	}

	rf.appendLogEntriesSince(args.PrevLogIndex+1, args.LogEntries)
	rf.commitIndex = args.CommitIndex
	rf.applyLogs()
	rf.resetElectionTimeoutTicks()

	newLastIndex, _ := rf.lastLogInfo()
	reply.Success = true
	reply.Message = "success"
	reply.LastLogIndex = newLastIndex
	return reply
}

func (rf *Raft) processAppendEntriesReply(reply *AppendEntriesReply) {
	if reply.Term > rf.term {
		// rf.becomeFollower()
		return
	}

	if !reply.Success {
		if rf.nextIndex[reply.PeerID] > 0 {
			rf.nextIndex[reply.PeerID]--
		}
		return
	}

	rf.nextIndex[reply.PeerID] = reply.LastLogIndex + 1
	rf.matchIndex[reply.PeerID] = reply.LastLogIndex

	rf.matchIndex[rf.me], _ = rf.lastLogInfo()
	commitIndex := calculateLeaderCommitIndex(rf.matchIndex)

	log.Printf("%v process-append-entries-reply matchIndex=%#v new-commit-index=%d", rf.logPrefix(), rf.matchIndex, commitIndex)
	if rf.commitIndex < commitIndex {
		rf.commitIndex = commitIndex
	}

	rf.applyLogs()
}

func (rf *Raft) processRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	reply := &RequestVoteReply{
		VoteGranted: false,
		Term:        rf.term,
		PeerID:      rf.me,
		Message:     "unexpected failure",
	}

	defer func() {
		log.Printf("%v process-request-vote reply=%s", rf.logPrefix(), reply.Message)
	}()

	// if the caller's term smaller than mine, simply refuse
	if args.Term < rf.term {
		reply.Message = fmt.Sprintf("args.Term: %d < rf.rterm: %d", args.Term, rf.term)
		return reply
	}

	// if the term is equal and we've already voted for another candidate
	if args.Term == rf.term && rf.votedFor >= 0 && rf.votedFor != args.CandidateID {
		reply.Message = fmt.Sprintf("i've already voted for %d in term %d", rf.votedFor, rf.term)
		return reply
	}

	// if the caller's term bigger than my term: set currentTerm = T, convert to follower
	if args.Term > rf.term {
		log.Printf("%v processwrequest-vote:term-bigger-than-me vote-for=%v", rf.logPrefix(), args.CandidateID)
		rf.becomeFollower()
		rf.term = args.Term
		rf.votedFor = args.CandidateID
		rf.persist()
	}

	// - If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// - If the logs end with the same term, then whichever log is longer is more up-to-date
	lastIndex, lastLogTerm := rf.lastLogInfo()
	if lastLogTerm > args.LastLogTerm {
		reply.Message = "candidate's log term not as updated as our last log"
		return reply
	} else if lastLogTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
		reply.Message = "candidate's log index not as updated as our last log"
		return reply
	}

	rf.votedFor = args.CandidateID
	rf.persist()
	reply.Term = rf.term
	reply.VoteGranted = true
	reply.Message = "cheers"
	return reply
}

func (rf *Raft) broadcastRequestVote() {
	logIndex, logTerm := rf.lastLogInfo()
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateID:  rf.me,
		LastLogIndex: logIndex,
		LastLogTerm:  logTerm,
	}

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}

		// async calling requestVote
		go func(peerID int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peerID, args, &reply)
			// log.Printf("%v candidate.sent-request-vote [%v] ok=%v args=%#v reply=%#v", rf.logPrefix(), peerID, ok, args, reply)
			if ok {
				rf.eventc <- newRaftEV(&reply)
			}
		}(peerID)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for peerID, nextIndex := range rf.nextIndex {
		if peerID == rf.me {
			continue
		}

		args := rf.prepareAppendEntriesArgs(nextIndex)

		go func(peerID int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peerID, args, &reply)
			// log.Printf("%v heartbeat [%v] reply=%#v", rf.logPrefix(), peerID, reply)

			emptyReply := AppendEntriesReply{}
			if ok && reply != emptyReply {
				rf.eventc <- newRaftEV(&reply)
			}
		}(peerID)
	}
}

func (rf *Raft) prepareAppendEntriesArgs(nextIndex int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		LeaderID:    rf.me,
		Term:        rf.term,
		CommitIndex: rf.commitIndex,
		LogEntries:  []raftLogEntry{},
	}
	if nextIndex == 0 {
		args.PrevLogIndex = -1
		args.PrevLogTerm = 0
		args.LogEntries = rf.logEntriesSince(0)
	} else {
		logEntries := rf.logEntriesSince(nextIndex - 1)
		if len(logEntries) >= 1 {
			args.PrevLogIndex = logEntries[0].Index
			args.PrevLogTerm = logEntries[0].Term
		}
		if len(logEntries) >= 2 {
			args.LogEntries = logEntries[1:]
		}
	}
	return args
}

func (rf *Raft) logPrefix() string {
	lastIndex, _ := rf.lastLogInfo()
	return fmt.Sprintf("[%d %v term:%d applyIdx:%d commitIdx:%d electionTicks:%d lastIndex:%#v]", rf.me, rf.state.String(), rf.term, rf.applyIndex, rf.commitIndex, rf.electionTimeoutTicks, lastIndex)
}

func calculateLeaderCommitIndex(matchIndex []int) int {
	indices := []int{}
	for _, idx := range matchIndex {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] >= indices[j] })
	return indices[len(indices)/2]
}
