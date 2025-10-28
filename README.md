# 6.5840 Distributed Systems Labs (Spring 2025)

Implementation of four fundamental distributed systems labs from MIT's 6.5840 course.

## Lab Overview

### Lab 1: MapReduce

Build a distributed MapReduce system with a coordinator and workers that execute map/reduce tasks in parallel.

**Key Features:**
- Coordinator distributes tasks and handles worker failures (10s timeout)
- Workers execute map tasks (partition data) and reduce tasks (aggregate results)
- Fault tolerance through task reassignment and atomic file operations
- Intermediate files: `mr-X-Y` (X=map task, Y=reduce task)
- Final output: `mr-out-X` (X=reduce task)

**Running:**
```bash
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
go run mrcoordinator.go pg-*.txt         # Terminal 1
go run mrworker.go wc.so                 # Terminal 2+
```

**Testing:**
```bash
cd src/main
bash test-mr.sh
```

---

### Lab 2: Key/Value Server

Build a single-machine key/value server that ensures **at-most-once** execution and **linearizability** despite network failures.

**Key Features:**
- `Put(key, value, version)` - conditional write with version check
- `Get(key)` - returns (value, version)
- Handles duplicate RPCs from retries
- Implements a distributed lock using the KV server
- Works with unreliable networks

**Files to Modify:**
- `src/kvsrv1/server.go` - KV server implementation
- `src/kvsrv1/rpc/rpc.go` - RPC definitions
- `src/kvsrv1/lock/lock.go` - Lock implementation using KV clerk

**Testing:**
```bash
cd src/kvsrv1
go test -v

cd src/kvsrv1/lock
go test -v
```

---

### Lab 3: Raft Consensus

Implement the Raft consensus algorithm for replicated state machines with leader election and log replication.

**Key Features:**
- Leader election with randomized timeouts
- Log replication from leader to followers
- Safety: committed entries never lost
- Persistence: survives crashes and reboots
- Log compaction with snapshots

**Components:**
- Leader election
- Log replication
- Persistence (state, log, snapshots)
- Log compaction/snapshots

**Files to Modify:**
- `src/raft1/raft.go` - Raft implementation
- `src/raft1/util.go` - Utilities

**Testing:**
```bash
cd src/raft1
go test -v
```

---

### Lab 4: Fault-Tolerant KV Service (KVRaft)

Build a fault-tolerant key/value service using Raft for replication across multiple servers.

**Key Features:**
- Replicated state machine using Raft
- Linearizable reads and writes
- Handles duplicate detection across crashes
- Works with network partitions
- Supports snapshots for log compaction

**Architecture:**
- Clerks (clients) send RPCs to KV servers
- KV servers use Raft for consensus
- Operations only complete when committed by Raft

**Files to Modify:**
- `src/kvraft1/rsm/server.go` - KV server with Raft
- `src/kvraft1/client.go` - KV client/clerk
- `src/kvraft1/rsm/rsm.go` - Replicated state machine

**Testing:**
```bash
cd src/kvraft1/rsm
go test -v
```

---

## Development Setup

### Prerequisites
- Go 1.17 or later
- Unix-like environment (Linux, macOS)

### Installation
```bash
git clone git://g.csail.mit.edu/6.5840-golabs-2025 6.5840
cd 6.5840
```

### Build Tips
- Use `-race` flag to detect race conditions: `go test -race`
- Use `DPrintf()` for debug logging that can be toggled
- Read test code to understand expected behavior

---

## Testing Strategy

### Run Single Test
```bash
go test -run TestBasic
```

### Run with Race Detector
```bash
go test -race
```

### Run Multiple Times (catch flaky bugs)
```bash
for i in {1..100}; do go test || break; done
```

### Verbose Output
```bash
go test -v
```

---

## Key Concepts by Lab

| Lab | Key Concepts |
|-----|-------------|
| **MapReduce** | Parallel processing, fault tolerance, task scheduling |
| **KVServer** | At-most-once semantics, linearizability, duplicate detection |
| **Raft** | Consensus, leader election, log replication, persistence |
| **KVRaft** | Replicated state machines, fault tolerance, snapshots |

---

## Common Debugging Tips

1. **Use logging extensively** - Add timestamps and server IDs
2. **Test with small inputs first** - Easier to trace execution
3. **Check for race conditions** - Always run with `-race` flag
4. **Read the papers** - MapReduce and Raft papers are essential
5. **Understand the test cases** - They reveal expected behavior
6. **Use Go's defer** - For cleanup and unlocking mutexes
7. **Lock granularity matters** - Hold locks as briefly as possible

---

## Progress Checklist

- [x] Lab 1: MapReduce - All tests passing
- [ ] Lab 2: Key/Value Server
- [ ] Lab 3: Raft Consensus
- [ ] Lab 4: Fault-Tolerant KV Service

---

## Resources

- [MIT 6.5840 Course Website](https://pdos.csail.mit.edu/6.824/)
- [MapReduce Paper](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)
- [Raft Paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)
- [Raft Visualization](https://raft.github.io/)
- [Go Documentation](https://golang.org/doc/)

---

## License

Educational use only - MIT 6.5840 course materials.

