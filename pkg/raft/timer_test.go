package raft

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestTickStartsElectionAfterTimeout(t *testing.T) {
	// No registered peers: elections cannot reach a majority.
	n := newElectionNodeWithTransport(t, newTestTransport(), "n2", "n3")

	ctx := context.Background()

	if err := n.Tick(ctx, 149*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	assertElectionStatus(t, n, 0, "Follower", "")

	if err := n.Tick(ctx, 152*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	assertElectionStatus(t, n, 1, "Candidate", "")
	// A failed round must not prevent another election timeout.
	if err := n.Tick(ctx, 301*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	assertElectionStatus(t, n, 2, "Candidate", "")
}

func TestTickHeartbeatResetsElectionTimeout(t *testing.T) {
	n := newElectionNodeWithTransport(t, newTestTransport(), "n2", "n3")

	ctx := context.Background()

	for range 10 {
		if err := n.Tick(ctx, 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}

		reply := n.HandleAppendEntries(AppendEntriesArgs{Term: 1, LeaderID: "n2"})
		if !reply.Success {
			t.Fatalf("heartbeat rejected: %+v", reply)
		}

		assertElectionStatus(t, n, 1, "Follower", "n2")
	}
	// After heartbeats stop, a new election must eventually begin.
	if err := n.Tick(ctx, 301*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	assertElectionStatus(t, n, 2, "Candidate", "")
}

func TestTickLeaderSendsPeriodicHeartbeats(t *testing.T) {
	var calls atomic.Int32

	tr := heartbeatTransportFunc(func(_ context.Context, _ string, a AppendEntriesArgs) (AppendEntriesReply, error) {
		calls.Add(1)
		return AppendEntriesReply{Term: a.Term, Success: true}, nil
	})
	n := newElectionNodeWithTransport(t, tr, "n2", "n3")
	term := electHeartbeatLeader(n)

	ctx := context.Background()
	if err := n.Tick(ctx, 49*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	if got := calls.Load(); got != 0 {
		t.Fatalf("heartbeat sent too early: %d calls", got)
	}

	if err := n.Tick(ctx, time.Millisecond); err != nil {
		t.Fatal(err)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}

	if err := n.Tick(ctx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	if got := calls.Load(); got != 4 {
		t.Fatalf("calls = %d, want 4", got)
	}

	assertElectionStatus(t, n, term, "Leader", "n1")
}

func TestTickRequestsVotes(t *testing.T) {
	tr := newTestTransport()
	n := newElectionNodeWithTransport(t, tr, "n2", "n3")

	peer, err := NewNode(Config{ID: "n2"}, tr)
	if err != nil {
		t.Fatal(err)
	}

	tr.Register("n2", peer)

	if err := n.Tick(context.Background(), 301*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	assertElectionStatus(t, n, 1, "Leader", "n1")
}

func TestRejectedRequestsDoNotResetTimer(t *testing.T) {
	for _, kind := range []string{"old heartbeat", "old vote", "second candidate"} {
		t.Run(kind, func(t *testing.T) {
			n := newElectionNodeWithTransport(t, newTestTransport(), "n2", "n3")
			n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "n2"})

			if err := n.Tick(context.Background(), 149*time.Millisecond); err != nil {
				t.Fatal(err)
			}

			switch kind {
			case "old heartbeat":
				n.HandleAppendEntries(AppendEntriesArgs{Term: 0, LeaderID: "n3"})
			case "old vote":
				n.HandleRequestVote(RequestVoteArgs{Term: 0, CandidateID: "n3"})
			default:
				n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "n3"})
			}

			if err := n.Tick(context.Background(), 152*time.Millisecond); err != nil {
				t.Fatal(err)
			}

			assertElectionStatus(t, n, 2, "Candidate", "")
		})
	}
}

func TestCanceledTickDoesNotChangeState(t *testing.T) {
	n := newElectionNodeWithTransport(t, newTestTransport(), "n2", "n3")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := n.Tick(ctx, 301*time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context.Canceled", err)
	}

	assertElectionStatus(t, n, 0, "Follower", "")
}
