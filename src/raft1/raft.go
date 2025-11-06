package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// LogEntry represents a single entry in the log
type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers (updated on stable storage before responding to RPCs)
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Additional state
	state            int       // 0: follower, 1: candidate, 2: leader
	lastHeartbeat    time.Time // last time we received/sent a heartbeat
	electionTimeout  time.Duration

	// Snapshot state (3D)
	lastIncludedIndex int    // index of last entry in snapshot
	lastIncludedTerm  int    // term of lastIncludedIndex
	snapshot           []byte // snapshot data

	applyCh chan raftapi.ApplyMsg // channel to send ApplyMsg to service
}

const (
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == LEADER)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// error...
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
	snapshot := rf.persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		rf.snapshot = snapshot
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	oldLastIncludedIndex := rf.lastIncludedIndex
	
	term := rf.getLogEntry(index).Term
	
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	
	entriesToKeep := index - oldLastIncludedIndex
	if entriesToKeep > 0 && entriesToKeep < len(rf.log) {
		rf.log = rf.log[entriesToKeep:]
	} else if entriesToKeep >= len(rf.log) {
		rf.log = []LogEntry{}
	}
	
	rf.snapshot = snapshot

	rf.persist()
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	if index == 0 {
		return LogEntry{Term: 0}
	}
	if index <= rf.lastIncludedIndex {
		return LogEntry{Term: rf.lastIncludedTerm}
	}
	return rf.log[index-rf.lastIncludedIndex-1]
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your code here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.Term == rf.currentTerm {
		lastLogIndex := rf.getLastLogIndex()
		lastLogTerm := rf.getLastLogTerm()
		
		if args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.state = FOLLOWER
			rf.resetElectionTimer()
			rf.persist()
		}
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

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId    int        // so follower can redirect clients
	PrevLogIndex int       // index of log entry immediately preceding new ones
	PrevLogTerm  int       // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int       // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // term in the conflicting entry (if any)
	XIndex  int  // index of first entry with that term (if any)
	XLen    int  // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}

	rf.resetElectionTimer()

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > rf.getLastLogIndex() {
			reply.XLen = rf.getLastLogIndex()
			return
		}

		prevLogEntry := rf.getLogEntry(args.PrevLogIndex)
		if prevLogEntry.Term != args.PrevLogTerm {
			reply.XTerm = prevLogEntry.Term
			for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
				if rf.getLogEntry(i).Term == reply.XTerm {
					reply.XIndex = i
				} else {
					break
				}
			}
			if reply.XIndex == 0 {
				reply.XIndex = rf.lastIncludedIndex + 1
			}
			return
		}
	}

	conflictIndex := -1
	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex + 1 + i
		if logIndex <= rf.getLastLogIndex() {
			if rf.getLogEntry(logIndex).Term != entry.Term {
				conflictIndex = logIndex
				break
			}
		} else {
			break
		}
	}

	if conflictIndex != -1 {
		logArrayIndex := conflictIndex - rf.lastIncludedIndex - 1
		if logArrayIndex >= 0 && logArrayIndex < len(rf.log) {
			rf.log = rf.log[:logArrayIndex]
		}
	}

	if len(args.Entries) > 0 {
		startIndex := args.PrevLogIndex + 1
		lastLogIndex := rf.getLastLogIndex()
		if startIndex <= lastLogIndex {
			logArrayIndex := startIndex - rf.lastIncludedIndex - 1
			if logArrayIndex >= 0 && logArrayIndex <= len(rf.log) {
				rf.log = rf.log[:logArrayIndex]
			}
		}
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
	}

	reply.Success = true

		if args.LeaderCommit > rf.commitIndex {
			lastNewEntryIndex := rf.getLastLogIndex()
			if args.LeaderCommit < lastNewEntryIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastNewEntryIndex
			}
		}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset 0
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}

	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	oldLastIncludedIndex := rf.lastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	
	entriesToKeep := args.LastIncludedIndex - oldLastIncludedIndex
	if entriesToKeep > 0 && entriesToKeep < len(rf.log) {
		rf.log = rf.log[entriesToKeep:]
	} else if entriesToKeep >= len(rf.log) {
		rf.log = []LogEntry{}
	}
	
	rf.snapshot = args.Data
	
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.persist()

	rf.mu.Unlock()
	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Lock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	if rf.state == LEADER && rf.killed() == false {
		entry := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		}
		rf.log = append(rf.log, entry)
		rf.persist()
		
		index = rf.getLastLogIndex()
		term = rf.currentTerm
		isLeader = true
	}

	return index, term, isLeader
}

func (rf *Raft) applyEntries() {
	if rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		entry := rf.getLogEntry(rf.lastApplied)
		
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.lastApplied,
		}
		
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	
	if rf.state == LEADER || rf.killed() {
		rf.mu.Unlock()
		return
	}

	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	currentTerm := rf.currentTerm

	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	votes := 1
	votesCh := make(chan bool, len(rf.peers))

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			
			rf.mu.Lock()
			stillCandidate := (rf.state == CANDIDATE && rf.currentTerm == currentTerm)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = FOLLOWER
				rf.persist()
			}
			rf.mu.Unlock()
			
			if ok && reply.VoteGranted && stillCandidate && reply.Term == currentTerm {
				votesCh <- true
			} else {
				votesCh <- false
			}
		}(i)
	}

	// Collect votes
	votesNeeded := len(rf.peers)/2 + 1
	timeout := time.After(200 * time.Millisecond)
	votesReceived := 0
	
	collecting:
		for votesReceived < len(rf.peers)-1 && votes < votesNeeded {
			select {
			case vote := <-votesCh:
				votesReceived++
				if vote {
					votes++
				}
			case <-timeout:
				break collecting
			}
		}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == CANDIDATE && rf.currentTerm == currentTerm && votes >= votesNeeded {
		rf.state = LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
		rf.resetHeartbeatTimer()
		rf.mu.Unlock()
		rf.sendHeartbeats()
		rf.mu.Lock()
	}
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	
	if rf.state != LEADER || rf.killed() {
		rf.mu.Unlock()
		return
	}

	rf.resetHeartbeatTimer()
	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesToFollower(i, currentTerm, leaderCommit)
	}
}

func (rf *Raft) sendAppendEntriesToFollower(server int, currentTerm int, leaderCommit int) {
	rf.mu.Lock()

	if rf.state != LEADER || rf.currentTerm != currentTerm || rf.killed() {
		rf.mu.Unlock()
		return
	}

	nextIndex := rf.nextIndex[server]
	lastLogIndex := rf.getLastLogIndex()

	if nextIndex <= rf.lastIncludedIndex {
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshot,
		}
		rf.mu.Unlock()

		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, &args, &reply)

		rf.mu.Lock()
		if ok && reply.Term == rf.currentTerm {
			rf.nextIndex[server] = rf.lastIncludedIndex + 1
			rf.matchIndex[server] = rf.lastIncludedIndex
		} else if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.persist()
		}
		rf.mu.Unlock()
		return
	}

	prevLogIndex := nextIndex - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = rf.getLogEntry(prevLogIndex).Term
	}

	entries := []LogEntry{}
	if nextIndex <= lastLogIndex {
		entries = rf.log[nextIndex-rf.lastIncludedIndex-1:]
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != LEADER || rf.currentTerm != currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		return
	}

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		for n := rf.commitIndex + 1; n <= rf.getLastLogIndex(); n++ {
			if rf.getLogEntry(n).Term != rf.currentTerm {
				continue
			}
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
			} else {
				break
			}
		}
	} else {
		if reply.XTerm > 0 {
			found := false
			for i := rf.getLastLogIndex(); i > rf.lastIncludedIndex; i-- {
				if rf.getLogEntry(i).Term == reply.XTerm {
					rf.nextIndex[server] = i + 1
					found = true
					break
				}
			}
			if !found {
				rf.nextIndex[server] = reply.XIndex
			}
		} else {
			rf.nextIndex[server] = reply.XLen
		}
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)

		rf.mu.Lock()
		state := rf.state
		now := time.Now()
		
		if state == FOLLOWER || state == CANDIDATE {
			if now.Sub(rf.lastHeartbeat) >= rf.electionTimeout {
				rf.mu.Unlock()
				rf.startElection()
				rf.mu.Lock()
				rf.resetElectionTimer()
			}
		} else if state == LEADER {
			if now.Sub(rf.lastHeartbeat) >= 100*time.Millisecond {
				rf.resetHeartbeatTimer()
				rf.mu.Unlock()
				rf.sendHeartbeats()
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()

		time.Sleep(50 * time.Millisecond)
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapshot = nil
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.resetElectionTimer()

	go rf.ticker()

	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			if rf.lastApplied < rf.commitIndex {
				rf.applyEntries()
			}
			rf.mu.Unlock()
			
			time.Sleep(10 * time.Millisecond)
		}
	}()

	return rf
}
