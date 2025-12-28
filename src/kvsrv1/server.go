package kvsrv

import (
	"log"
	"net/rpc"
	"sync"

	kvrpc "6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueEntry struct {
	Value   string
	Version int
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]ValueEntry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.data = make(map[string]ValueEntry)
	rpc.Register(kv)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *kvrpc.GetArgs, reply *kvrpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.data[args.Key]
	if !ok {
		reply.Err = kvrpc.ErrNoKey
		return
	}
	reply.Value = entry.Value
	reply.Version = kvrpc.Tversion(entry.Version)
	reply.Err = kvrpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *kvrpc.PutArgs, reply *kvrpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.data[args.Key]
	if !ok {
		if args.Version != 0 {
			reply.Err = kvrpc.ErrNoKey
			return
		}
		kv.data[args.Key] = ValueEntry{Value: args.Value, Version: 1}
		reply.Err = kvrpc.OK
		return
	}

	if entry.Version != int(args.Version) {
		reply.Err = kvrpc.ErrVersion
		return
	}
	kv.data[args.Key] = ValueEntry{Value: args.Value, Version: entry.Version + 1}
	reply.Err = kvrpc.OK

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
