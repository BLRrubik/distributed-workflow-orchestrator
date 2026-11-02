package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrStopped = errors.New("raft node stopped")
)

type NodeStatus struct {
	ID       string `json:"id"`
	LeaderID string `json:"leader_id"`
	State    string `json:"state"`
	Term     uint64 `json:"term"`
}

type Node struct {
	cfg             Config
	state           NodeState
	persistentState PersistentState
	leaderID        string
	votes           map[string]struct{}
	peers           map[string]struct{}
	transport       Transport

	electionElapsed  time.Duration
	electionTimeout  time.Duration
	heartbeatElapsed time.Duration

	stopped bool
	cancel  context.CancelFunc
	done    chan struct{}

	mu sync.Mutex
}

func NewNode(cfg Config, transport Transport) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %v", err)
	}

	node := &Node{
		cfg:             cfg,
		state:           Follower,
		persistentState: PersistentState{},
		peers:           map[string]struct{}{},
		transport:       transport,
		done:            make(chan struct{}),
	}

	for _, peer := range cfg.Peers {
		node.peers[peer] = struct{}{}
	}

	node.resetElectionTimerLocked()

	return node, nil
}

func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.stopped {
		return ErrStopped
	}

	if n.cancel != nil {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel

	go n.run(ctx)

	return nil
}

func (n *Node) Stop() {
	n.mu.Lock()
	n.stopped = true
	cancel := n.cancel
	n.mu.Unlock()

	if cancel == nil {
		return
	}

	cancel()
	<-n.done
}

func (n *Node) Status() NodeStatus {
	n.mu.Lock()
	defer n.mu.Unlock()

	return NodeStatus{
		ID:       n.cfg.ID,
		State:    n.state.String(),
		LeaderID: n.leaderID,
		Term:     n.persistentState.CurrentTerm,
	}
}

func (n *Node) getLastLogLocked() LogEntry {
	if len(n.persistentState.Log) == 0 {
		return LogEntry{}
	}

	return n.persistentState.Log[len(n.persistentState.Log)-1]
}

func (n *Node) getQuorumLocked() int {
	clusterSize := len(n.cfg.Peers) + 1

	return clusterSize/2 + 1
}

func (n *Node) changeToFollowerLocked(term uint64) {
	n.persistentState.CurrentTerm = term
	n.persistentState.VotedFor = ""
	n.state = Follower
	n.leaderID = ""
}

func (n *Node) changeToLeaderLocked() {
	n.state = Leader
	n.leaderID = n.cfg.ID

	n.resetHeartbeatTimerLocked()
}

func (n *Node) run(ctx context.Context) {
	defer close(n.done)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	previous := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := time.Now()
			elapsed := current.Sub(previous)
			previous = current

			if err := n.Tick(ctx, elapsed); err != nil {
				return
			}
		}
	}
}
