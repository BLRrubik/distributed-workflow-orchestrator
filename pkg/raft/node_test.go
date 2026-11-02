package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func waitNodeCondition(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}

		select {
		case <-deadline.C:
			t.Fatal("node did not reach expected state within 2 seconds")
		case <-ticker.C:
		}
	}
}

func TestStartAutomaticallyElectsSingleNodeAndStopFreezesProgress(t *testing.T) {
	n := newElectionNode(t)
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { stopNodeWithTimeout(t, n) })

	if err := n.Start(); err != nil {
		t.Fatalf("repeated Start should be harmless: %v", err)
	}

	waitNodeCondition(t, func() bool { return n.Status().State == "Leader" })
	assertElectionStatus(t, n, 1, "Leader", "n1")
	stopNodeWithTimeout(t, n)
	stopNodeWithTimeout(t, n)

	if err := n.Start(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Start after Stop error=%v, want ErrStopped", err)
	}
}

func TestStopBeforeStartPreventsStart(t *testing.T) {
	n := newElectionNode(t)
	stopNodeWithTimeout(t, n)
	stopNodeWithTimeout(t, n)

	if err := n.Start(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Start after Stop error=%v, want ErrStopped", err)
	}

	assertElectionStatus(t, n, 0, "Follower", "")
}

func TestStopCancelsAndWaitsForElectionRequests(t *testing.T) {
	entered := make(chan struct{})

	var (
		once     sync.Once
		requests sync.WaitGroup
	)

	tr := voteTransportFunc(func(ctx context.Context, _ string, _ RequestVoteArgs) (RequestVoteReply, error) {
		requests.Add(1)
		defer requests.Done()

		once.Do(func() { close(entered) })
		<-ctx.Done()

		return RequestVoteReply{}, ctx.Err()
	})

	n := newElectionNodeWithTransport(t, tr, "n2", "n3")
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { stopNodeWithTimeout(t, n) })

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("election did not reach transport")
	}

	stopNodeWithTimeout(t, n)
	// Stop must return only after the loop and all its RPCs have finished.
	waitWithTimeout(t, "election requests to finish", requests.Wait)

	status := n.Status()
	if status.Term != 1 || status.State != "Candidate" {
		t.Fatalf("unexpected state after canceled election: %+v", status)
	}
}

func TestConfigAndNodeCreation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "single node", cfg: Config{ID: "node-1"}},
		{name: "empty peer list", cfg: Config{ID: "node-1", Peers: []string{}}},
		{name: "three node cluster", cfg: Config{ID: "node-1", Peers: []string{"node-2", "node-3"}}},
		{name: "empty node ID", cfg: Config{}, wantErr: true},
		{name: "negative RPC timeout", cfg: Config{ID: "node-1", RPCTimeout: -time.Millisecond}, wantErr: true},
		{name: "empty peer ID", cfg: Config{ID: "node-1", Peers: []string{"node-2", ""}}, wantErr: true},
		{name: "self among peers", cfg: Config{ID: "node-1", Peers: []string{"node-2", "node-1"}}, wantErr: true},
		{name: "adjacent duplicate", cfg: Config{ID: "node-1", Peers: []string{"node-2", "node-2"}}, wantErr: true},
		{name: "nonadjacent duplicate", cfg: Config{ID: "node-1", Peers: []string{"node-2", "node-3", "node-2"}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, want error: %v", err, tt.wantErr)
			}

			node, err := NewNode(tt.cfg, nil)
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewNode(, nil) accepted invalid config")
				}

				if node != nil {
					t.Error("NewNode(, nil) returned a node for invalid config")
				}

				return
			}

			if err != nil {
				t.Fatalf("NewNode(, nil) failed: %v", err)
			}

			if node == nil {
				t.Fatal("NewNode(, nil) returned nil without error")
			}

			want := NodeStatus{ID: tt.cfg.ID, State: "Follower", Term: 0, LeaderID: ""}
			if got := node.Status(); got != want {
				t.Errorf("initial Status() = %+v, want %+v", got, want)
			}

			// Changing a returned snapshot must not change the node.
			snapshot := node.Status()
			snapshot.ID = "changed"
			snapshot.State = "Leader"
			snapshot.LeaderID = "other"
			snapshot.Term = 99

			if got := node.Status(); got != want {
				t.Errorf("Status() changed after modifying snapshot: %+v, want %+v", got, want)
			}
		})
	}
}

func waitWithTimeout(t *testing.T, operation string, fn func()) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		fn()
		close(done)
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s after 2 seconds", operation)
	}
}

func stopNodeWithTimeout(t *testing.T, n *Node) {
	t.Helper()
	waitWithTimeout(t, "Stop()", n.Stop)
}
