package inmemory

import (
	"context"
	"fmt"
	"sync"

	"github.com/blrrubik/distributed-workflow-orchestrator/pkg/raft"
)

type MemoryTransport struct {
	peers map[string]raft.Receiver
	mu    sync.RWMutex
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		peers: make(map[string]raft.Receiver),
	}
}

func (t *MemoryTransport) Register(id string, receiver raft.Receiver) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.peers[id] = receiver
}

func (t *MemoryTransport) Unregister(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.peers, id)
}

func (t *MemoryTransport) RequestVote(
	ctx context.Context,
	to string,
	args raft.RequestVoteArgs,
) (raft.RequestVoteReply, error) {
	if ctx.Err() != nil {
		return raft.RequestVoteReply{}, fmt.Errorf("vote request canceled: %w", ctx.Err())
	}

	t.mu.RLock()
	receiver, ok := t.peers[to]
	t.mu.RUnlock()

	if !ok {
		return raft.RequestVoteReply{}, fmt.Errorf("peer %s not found", to)
	}

	return receiver.HandleRequestVote(args), nil
}

func (t *MemoryTransport) AppendEntries(
	ctx context.Context,
	to string,
	args raft.AppendEntriesArgs,
) (raft.AppendEntriesReply, error) {
	if ctx.Err() != nil {
		return raft.AppendEntriesReply{}, fmt.Errorf("heartbeat canceled: %w", ctx.Err())
	}

	t.mu.RLock()
	receiver, ok := t.peers[to]
	t.mu.RUnlock()

	if !ok {
		return raft.AppendEntriesReply{}, fmt.Errorf("peer %s not found", to)
	}

	return receiver.HandleAppendEntries(args), nil
}
