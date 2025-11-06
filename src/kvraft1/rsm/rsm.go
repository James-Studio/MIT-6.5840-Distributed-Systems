package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id      int64
	Me      int
	Request any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	opIdCounter int64
	pendingOps  map[int]chan applyResult
	lastApplied int
	dead        int32
}

type applyResult struct {
	err  rpc.Err
	data any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pendingOps:   make(map[int]chan applyResult),
		lastApplied:  0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	
	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		sm.Restore(snapshot)
	}
	
	go rsm.reader()
	
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) reader() {
	for !rsm.killed() {
		select {
		case msg, ok := <-rsm.applyCh:
			if !ok {
				rsm.mu.Lock()
				for index, ch := range rsm.pendingOps {
					select {
					case ch <- applyResult{err: rpc.ErrWrongLeader, data: nil}:
					default:
					}
					delete(rsm.pendingOps, index)
				}
				rsm.mu.Unlock()
				return
			}
			
			rsm.mu.Lock()
		
		if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
			rsm.lastApplied = msg.SnapshotIndex
			rsm.mu.Unlock()
			continue
		}
		
		if !msg.CommandValid {
			rsm.mu.Unlock()
			continue
		}
		
		op, ok := msg.Command.(Op)
		if !ok {
			rsm.mu.Unlock()
			continue
		}
		
		result := rsm.sm.DoOp(op.Request)
		
		if ch, ok := rsm.pendingOps[msg.CommandIndex]; ok {
			ch <- applyResult{err: rpc.OK, data: result}
			delete(rsm.pendingOps, msg.CommandIndex)
		}
		
		rsm.lastApplied = msg.CommandIndex
		
		if rsm.maxraftstate > 0 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
			snapshot := rsm.sm.Snapshot()
			rsm.rf.Snapshot(msg.CommandIndex, snapshot)
		}
		
		rsm.mu.Unlock()
		case <-time.After(100 * time.Millisecond):
			if rsm.killed() {
				rsm.mu.Lock()
				for index, ch := range rsm.pendingOps {
					select {
					case ch <- applyResult{err: rpc.ErrWrongLeader, data: nil}:
					default:
					}
					delete(rsm.pendingOps, index)
				}
				rsm.mu.Unlock()
				return
			}
		}
	}
}

func (rsm *RSM) killed() bool {
	return atomic.LoadInt32(&rsm.dead) == 1
}

func (rsm *RSM) Kill() {
	atomic.StoreInt32(&rsm.dead, 1)
	
	rsm.mu.Lock()
	pending := make([]chan applyResult, 0, len(rsm.pendingOps))
	for index, ch := range rsm.pendingOps {
		pending = append(pending, ch)
		delete(rsm.pendingOps, index)
	}
	rsm.mu.Unlock()
	
	for _, ch := range pending {
		select {
		case ch <- applyResult{err: rpc.ErrWrongLeader, data: nil}:
		default:
		}
	}
	
	rsm.rf.Kill()
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	
	_, isLeader := rsm.rf.GetState()
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	
	opId := atomic.AddInt64(&rsm.opIdCounter, 1)
	op := Op{
		Id:      opId,
		Me:      rsm.me,
		Request: req,
	}
	
	index, startTerm, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	
	resultCh := make(chan applyResult, 1)
	rsm.pendingOps[index] = resultCh
	rsm.mu.Unlock()
	
	for {
		if rsm.killed() {
			rsm.mu.Lock()
			delete(rsm.pendingOps, index)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
		
		select {
		case result := <-resultCh:
			if rsm.killed() {
				return rpc.ErrWrongLeader, nil
			}
			currentTerm, stillLeader := rsm.rf.GetState()
			if result.err == rpc.OK && stillLeader && currentTerm == startTerm {
				return rpc.OK, result.data
			} else {
				return rpc.ErrWrongLeader, nil
			}
		case <-time.After(1 * time.Millisecond):
			if rsm.killed() {
				rsm.mu.Lock()
				delete(rsm.pendingOps, index)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			currentTerm, stillLeader := rsm.rf.GetState()
			if !stillLeader || currentTerm != startTerm {
				rsm.mu.Lock()
				delete(rsm.pendingOps, index)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
