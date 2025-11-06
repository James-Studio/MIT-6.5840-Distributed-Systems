package kvraft

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	mu      sync.Mutex
	leader  int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:    clnt,
		servers: servers,
		leader:  0,
	}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	args := &rpc.GetArgs{Key: key}
	reply := &rpc.GetReply{}

	ck.mu.Lock()
	leaderIndex := ck.leader
	ck.mu.Unlock()

	for i := 0; i < len(ck.servers); i++ {
		serverIndex := (leaderIndex + i) % len(ck.servers)
		
		ok := ck.clnt.Call(ck.servers[serverIndex], "KVServer.Get", args, reply)
		if ok {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
				ck.mu.Lock()
				ck.leader = serverIndex
				ck.mu.Unlock()
				return reply.Value, reply.Version, reply.Err
			} else if reply.Err == rpc.ErrWrongLeader {
				continue
			} else {
				return reply.Value, reply.Version, reply.Err
			}
		}
	}

	time.Sleep(100 * time.Millisecond)
	return ck.Get(key)
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := &rpc.PutReply{}

	ck.mu.Lock()
	leaderIndex := ck.leader
	ck.mu.Unlock()

	firstAttempt := true

	for i := 0; i < len(ck.servers); i++ {
		serverIndex := (leaderIndex + i) % len(ck.servers)
		
		ok := ck.clnt.Call(ck.servers[serverIndex], "KVServer.Put", args, reply)
		if ok {
			if reply.Err == rpc.OK {
				ck.mu.Lock()
				ck.leader = serverIndex
				ck.mu.Unlock()
				return rpc.OK
			} else if reply.Err == rpc.ErrVersion {
				if firstAttempt {
					return rpc.ErrVersion
				} else {
					return rpc.ErrMaybe
				}
			} else if reply.Err == rpc.ErrWrongLeader {
				firstAttempt = false
				continue
			} else {
				return reply.Err
			}
		}
		firstAttempt = false
	}

	time.Sleep(100 * time.Millisecond)
	return ck.Put(key, value, version)
}
