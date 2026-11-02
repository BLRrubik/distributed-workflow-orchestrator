package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSendHeartbeatsLimitsSlowPeerRPC(t *testing.T) {
	var successfulDone <-chan struct{}

	tr := heartbeatTransportFunc(func(ctx context.Context, to string, a AppendEntriesArgs) (AppendEntriesReply, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("outgoing heartbeat has no deadline")
			return AppendEntriesReply{}, errors.New("missing deadline")
		}

		if time.Until(deadline) > 30*time.Millisecond {
			t.Error("heartbeat deadline exceeds configured timeout")
			return AppendEntriesReply{}, errors.New("deadline too long")
		}

		if to == "n2" {
			<-ctx.Done()
			return AppendEntriesReply{}, ctx.Err()
		}

		successfulDone = ctx.Done()

		return AppendEntriesReply{Term: a.Term, Success: true}, nil
	})

	n, err := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}, RPCTimeout: 20 * time.Millisecond}, tr)
	if err != nil {
		t.Fatal(err)
	}

	term := electHeartbeatLeader(n)

	parent := t.Context()

	for range 2 {
		waitWithTimeout(t, "heartbeat with slow peer", func() {
			if err := n.SendHeartbeats(parent); err != nil {
				t.Errorf("round error: %v", err)
			}
		})
		assertElectionStatus(t, n, term, "Leader", "n1")

		if parent.Err() != nil {
			t.Fatal("RPC timeout canceled parent context")
		}

		select {
		case <-successfulDone:
		default:
			t.Fatal("successful RPC child context was not canceled after use")
		}
	}
}

func TestHeartbeatSameTermStepsDownAndPreservesVote(t *testing.T) {
	n := newElectionNode(t, "n2", "n3")

	a := n.StartElection()
	for range 2 {
		reply := n.HandleAppendEntries(AppendEntriesArgs{Term: a.Term, LeaderID: "n2"})
		if reply.Term != a.Term || !reply.Success {
			t.Fatalf("heartbeat reply = %+v, want term %d and success", reply, a.Term)
		}

		assertElectionStatus(t, n, a.Term, "Follower", "n2")
	}
	// Heartbeats in the same term must not erase the vote for ourselves.
	vote := n.HandleRequestVote(RequestVoteArgs{Term: a.Term, CandidateID: "n3"})
	if vote.VoteGranted {
		t.Fatal("same-term heartbeat erased the previously granted vote")
	}
	// Replies to the abandoned election cannot turn us into a leader.
	n.HandleRequestVoteReply("n3", a.Term, RequestVoteReply{Term: a.Term, VoteGranted: true})
	assertElectionStatus(t, n, a.Term, "Follower", "n2")
}

func TestHeartbeatHigherTermStepsDownAndClearsOldVote(t *testing.T) {
	n := newElectionNode(t, "n2", "n3")
	a := n.StartElection()
	n.HandleRequestVoteReply("n2", a.Term, RequestVoteReply{Term: a.Term, VoteGranted: true})
	assertElectionStatus(t, n, a.Term, "Leader", "n1")
	term := a.Term + 1

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: term, LeaderID: "n3"})
	if reply.Term != term || !reply.Success {
		t.Fatalf("heartbeat reply = %+v, want term %d and success", reply, term)
	}

	assertElectionStatus(t, n, term, "Follower", "n3")
	// The vote from the previous term must no longer prohibit a new vote.
	vote := n.HandleRequestVote(RequestVoteArgs{Term: term, CandidateID: "n3"})
	if vote.Term != term || !vote.VoteGranted {
		t.Fatalf("old vote was not cleared: %+v", vote)
	}
}

func TestHeartbeatOlderTermLeavesStateUnchanged(t *testing.T) {
	for _, role := range []string{"Candidate", "Leader", "Follower"} {
		t.Run(role, func(t *testing.T) {
			n := newElectionNode(t, "n2", "n3")
			a := n.StartElection()

			switch role {
			case "Leader":
				n.HandleRequestVoteReply("n2", a.Term, RequestVoteReply{Term: a.Term, VoteGranted: true})
			case "Follower":
				n.HandleAppendEntries(AppendEntriesArgs{Term: a.Term, LeaderID: "n2"})
			}

			before := n.Status()

			reply := n.HandleAppendEntries(AppendEntriesArgs{Term: a.Term - 1, LeaderID: "n3"})
			if reply.Term != a.Term || reply.Success {
				t.Fatalf("old heartbeat reply = %+v, want term %d and rejection", reply, a.Term)
			}

			if after := n.Status(); after != before {
				t.Fatalf("old heartbeat changed status: before=%+v after=%+v", before, after)
			}

			vote := n.HandleRequestVote(RequestVoteArgs{Term: a.Term, CandidateID: "n3"})
			if vote.VoteGranted {
				t.Fatal("old heartbeat erased the previously granted vote")
			}
		})
	}
}

type heartbeatTransportFunc func(context.Context, string, AppendEntriesArgs) (AppendEntriesReply, error)

func (f heartbeatTransportFunc) AppendEntries(ctx context.Context, to string, a AppendEntriesArgs) (AppendEntriesReply, error) {
	return f(ctx, to, a)
}

func (f heartbeatTransportFunc) RequestVote(context.Context, string, RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{}, errors.New("unexpected vote request")
}

func electHeartbeatLeader(n *Node) uint64 {
	a := n.StartElection()
	n.HandleRequestVoteReply("n2", a.Term, RequestVoteReply{Term: a.Term, VoteGranted: true})

	return a.Term
}

func TestSendHeartbeatsReachesFollowers(t *testing.T) {
	tr := newTestTransport()
	n := newElectionNodeWithTransport(t, tr, "n2", "n3")
	peers := make([]*Node, 0, 2)

	for _, id := range []string{"n2", "n3"} {
		peer, err := NewNode(Config{ID: id}, tr)
		if err != nil {
			t.Fatal(err)
		}

		peers = append(peers, peer)
		tr.Register(id, peer)
	}

	term := electHeartbeatLeader(n)

	if err := n.SendHeartbeats(context.Background()); err != nil {
		t.Fatal(err)
	}

	for _, peer := range peers {
		got := peer.Status()
		if got.State != "Follower" || got.Term != term || got.LeaderID != "n1" {
			t.Fatalf("unexpected follower status: %+v", got)
		}
	}
}

func TestSendHeartbeatsContactsAllPeersDespiteError(t *testing.T) {
	var (
		n  *Node
		mu sync.Mutex
	)

	calls := map[string]int{}
	tr := heartbeatTransportFunc(func(_ context.Context, to string, a AppendEntriesArgs) (AppendEntriesReply, error) {
		// A callback verifies the node mutex is not held during transport calls.
		status := n.Status()
		if a.Term != status.Term || a.LeaderID != "n1" || len(a.Entries) != 0 ||
			a.PrevLogIndex != 0 || a.PrevLogTerm != 0 || a.LeaderCommit != 0 {
			t.Errorf("unexpected heartbeat: %+v", a)
		}

		mu.Lock()
		calls[to]++
		mu.Unlock()

		if to == "n2" {
			return AppendEntriesReply{}, errors.New("unavailable")
		}

		return AppendEntriesReply{Term: a.Term, Success: true}, nil
	})
	n = newElectionNodeWithTransport(t, tr, "n2", "n3")

	term := electHeartbeatLeader(n)

	if err := n.SendHeartbeats(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(calls) != 2 || calls["n2"] != 1 || calls["n3"] != 1 {
		t.Fatalf("calls = %v, want one per peer", calls)
	}

	assertElectionStatus(t, n, term, "Leader", "n1")
}

func TestSendHeartbeatsHigherTermReplyStepsDown(t *testing.T) {
	tr := heartbeatTransportFunc(func(_ context.Context, _ string, a AppendEntriesArgs) (AppendEntriesReply, error) {
		return AppendEntriesReply{Term: a.Term + 2, Success: false}, nil
	})
	n := newElectionNodeWithTransport(t, tr, "n2", "n3")

	term := electHeartbeatLeader(n)

	if err := n.SendHeartbeats(context.Background()); err != nil {
		t.Fatal(err)
	}

	assertElectionStatus(t, n, term+2, "Follower", "")
}

func TestSendHeartbeatsNonLeaderDoesNothing(t *testing.T) {
	for _, candidate := range []bool{false, true} {
		n := newElectionNodeWithTransport(t, heartbeatTransportFunc(func(context.Context, string, AppendEntriesArgs) (AppendEntriesReply, error) {
			t.Error("non-leader sent a heartbeat")
			return AppendEntriesReply{}, nil
		}), "n2", "n3")
		if candidate {
			n.StartElection()
		}

		before := n.Status()

		if err := n.SendHeartbeats(context.Background()); err != nil {
			t.Fatal(err)
		}

		if after := n.Status(); after != before {
			t.Fatalf("non-leader state changed: before=%+v after=%+v", before, after)
		}
	}
}

func TestSendHeartbeatsCancellation(t *testing.T) {
	for _, cancelBefore := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		tr := heartbeatTransportFunc(func(ctx context.Context, _ string, _ AppendEntriesArgs) (AppendEntriesReply, error) {
			if cancelBefore {
				t.Error("already canceled call sent a heartbeat")
			}

			cancel()

			return AppendEntriesReply{}, ctx.Err()
		})
		n := newElectionNodeWithTransport(t, tr, "n2", "n3")
		term := electHeartbeatLeader(n)

		if cancelBefore {
			cancel()
		}

		err := n.SendHeartbeats(ctx)

		cancel()

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v, want context.Canceled", err)
		}

		assertElectionStatus(t, n, term, "Leader", "n1")
	}
}
