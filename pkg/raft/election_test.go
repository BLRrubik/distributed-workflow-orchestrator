package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSimultaneousElectionsSplitVoteAndRecoverNextRound(t *testing.T) {
	tr := newTestTransport()
	ids := []string{"n1", "n2", "n3"}

	var nodes map[string]*Node

	requests := make(map[string]RequestVoteArgs)
	nodes = createElectionCluster(t, tr, ids)
	// Hold all messages until everyone has voted for themselves in term 1.
	// This forces simultaneous elections without relying on goroutine scheduling.
	for _, id := range ids {
		requests[id] = nodes[id].StartElection()
	}

	ctx := context.Background()

	deliverSplitVotes(t, ctx, tr, ids, nodes, requests)

	for _, id := range ids {
		want := NodeStatus{ID: id, State: "Candidate", Term: 1}
		if got := nodes[id].Status(); got != want {
			t.Fatalf("split round status=%+v, want %+v", got, want)
		}
	}
	// n1 gets the next timeout first. Larger term lets peers cast fresh votes.
	if err := nodes["n1"].RunElection(ctx); err != nil {
		t.Fatal(err)
	}

	assertElectionStatus(t, nodes["n1"], 2, "Leader", "n1")

	if err := nodes["n1"].SendHeartbeats(ctx); err != nil {
		t.Fatal(err)
	}

	for _, id := range ids {
		role := "Follower"
		if id == "n1" {
			role = "Leader"
		}

		want := NodeStatus{ID: id, State: role, Term: 2, LeaderID: "n1"}
		if got := nodes[id].Status(); got != want {
			t.Fatalf("recovered status=%+v, want %+v", got, want)
		}
	}
	// Delayed requests and even granted replies from the old round cannot
	// revive an abandoned election or change the elected leader.
	for _, id := range ids {
		before := nodes[id].Status()
		for _, peer := range ids {
			if peer == id {
				continue
			}

			nodes[id].HandleRequestVoteReply(peer, 1, RequestVoteReply{Term: 1, VoteGranted: true})

			reply := nodes[id].HandleRequestVote(requests[peer])
			if reply.Term != 2 || reply.VoteGranted {
				t.Fatalf("old request accepted: %+v", reply)
			}
		}

		if after := nodes[id].Status(); after != before {
			t.Fatalf("old messages changed status: before=%+v after=%+v", before, after)
		}
	}
}

func TestRunElectionLimitsSlowPeerRPC(t *testing.T) {
	for _, configured := range []time.Duration{0, 20 * time.Millisecond} {
		t.Run(configured.String(), func(t *testing.T) {
			var successfulDone <-chan struct{}

			tr := voteTransportFunc(func(ctx context.Context, to string, a RequestVoteArgs) (RequestVoteReply, error) {
				deadline, ok := ctx.Deadline()
				if !ok {
					t.Error("outgoing vote request has no deadline")
					return RequestVoteReply{}, errors.New("missing deadline")
				}

				limit := configured
				if limit == 0 {
					limit = 50 * time.Millisecond
				}

				if time.Until(deadline) > limit+10*time.Millisecond {
					t.Error("vote deadline exceeds configured timeout")
					return RequestVoteReply{}, errors.New("deadline too long")
				}

				if to == "n2" {
					<-ctx.Done()
					return RequestVoteReply{}, ctx.Err()
				}

				successfulDone = ctx.Done()

				return RequestVoteReply{Term: a.Term, VoteGranted: true}, nil
			})

			n, err := NewNode(Config{ID: "n1", Peers: []string{"n2", "n3"}, RPCTimeout: configured}, tr)
			if err != nil {
				t.Fatal(err)
			}

			parent := t.Context()

			waitWithTimeout(t, "election with slow peer", func() {
				if err := n.RunElection(parent); err != nil {
					t.Errorf("round error: %v", err)
				}
			})
			assertElectionStatus(t, n, 1, "Leader", "n1")

			if parent.Err() != nil {
				t.Fatal("RPC timeout canceled parent context")
			}

			select {
			case <-successfulDone:
			default:
				t.Fatal("successful RPC child context was not canceled after use")
			}
		})
	}
}

func TestStartElectionCreatesNewRoundAndVotesForSelf(t *testing.T) {
	node, err := NewNode(Config{ID: "node-1", Peers: []string{"node-2", "node-3"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The node previously voted for somebody else in term 5.
	previous := node.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})
	if !previous.VoteGranted {
		t.Fatalf("setup vote rejected: %+v", previous)
	}

	for _, term := range []uint64{6, 7} {
		args := node.StartElection()

		wantArgs := RequestVoteArgs{Term: term, CandidateID: "node-1", LastLogTerm: 0, LastLogIndex: 0}
		if args != wantArgs {
			t.Fatalf("StartElection() = %+v, want %+v", args, wantArgs)
		}

		wantStatus := NodeStatus{ID: "node-1", State: "Candidate", Term: term}
		if got := node.Status(); got != wantStatus {
			t.Fatalf("Status() = %+v, want %+v", got, wantStatus)
		}
		// Observing votes through the RPC verifies the self-vote was saved.
		other := node.HandleRequestVote(RequestVoteArgs{Term: term, CandidateID: "node-3"})
		if other.Term != term || other.VoteGranted {
			t.Fatalf("another candidate received our self-vote: %+v", other)
		}

		self := node.HandleRequestVote(args)
		if self.Term != term || !self.VoteGranted {
			t.Fatalf("self-vote was not retained: %+v", self)
		}
	}
}

func TestRequestVoteOneVotePerTerm(t *testing.T) {
	node, err := NewNode(Config{ID: "node-1", Peers: []string{"node-2", "node-3"}}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Requests form one conversation with the same node; its vote must persist
	// between calls, and a new term must allow a new vote.
	steps := []struct {
		name      string
		term      uint64
		candidate string
		wantTerm  uint64
		wantVote  bool
	}{
		{"first vote", 1, "node-2", 1, true},
		{"same candidate retries", 1, "node-2", 1, true},
		{"second candidate rejected", 1, "node-3", 1, false},
		{"old term rejected", 0, "node-3", 1, false},
		{"new term permits another candidate", 2, "node-3", 2, true},
		{"previous candidate cannot get second vote", 2, "node-2", 2, false},
		{"old request cannot reset term or vote", 1, "node-2", 2, false},
		{"current candidate can still retry", 2, "node-3", 2, true},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			got := node.HandleRequestVote(RequestVoteArgs{
				Term:         step.term,
				CandidateID:  step.candidate,
				LastLogIndex: 0,
				LastLogTerm:  0,
			})
			if got.Term != step.wantTerm || got.VoteGranted != step.wantVote {
				t.Fatalf("reply = %+v, want Term=%d VoteGranted=%v", got, step.wantTerm, step.wantVote)
			}

			status := node.Status()
			if status.Term != step.wantTerm || status.State != "Follower" || status.LeaderID != "" {
				t.Fatalf("unexpected status after vote request: %+v", status)
			}
		})
	}
}

func newElectionNode(t *testing.T, peers ...string) *Node {
	t.Helper()

	n, err := NewNode(Config{ID: "n1", Peers: peers}, nil)
	if err != nil {
		t.Fatal(err)
	}

	return n
}

func assertElectionStatus(t *testing.T, n *Node, term uint64, role, leader string) {
	t.Helper()

	want := NodeStatus{ID: "n1", Term: term, State: role, LeaderID: leader}
	if got := n.Status(); got != want {
		t.Fatalf("Status() = %+v, want %+v", got, want)
	}
}

func TestVoteRepliesRequireUniqueMajority(t *testing.T) {
	n := newElectionNode(t, "n2", "n3", "n4", "n5")
	a := n.StartElection()
	yes := RequestVoteReply{Term: a.Term, VoteGranted: true}
	n.HandleRequestVoteReply("n2", a.Term, yes)
	assertElectionStatus(t, n, a.Term, "Candidate", "") // 2/5 including self
	n.HandleRequestVoteReply("n2", a.Term, yes)
	n.HandleRequestVoteReply("n1", a.Term, yes)
	n.HandleRequestVoteReply("unknown", a.Term, yes)
	n.HandleRequestVoteReply("n3", a.Term, RequestVoteReply{Term: a.Term})
	assertElectionStatus(t, n, a.Term, "Candidate", "")
	n.HandleRequestVoteReply("n4", a.Term, yes)
	assertElectionStatus(t, n, a.Term, "Leader", "n1") // 3/5
}

func TestVoteRepliesDoNotCarryOverBetweenRounds(t *testing.T) {
	n := newElectionNode(t, "n2", "n3", "n4", "n5")
	old := n.StartElection()
	n.HandleRequestVoteReply("n2", old.Term, RequestVoteReply{Term: old.Term, VoteGranted: true})
	current := n.StartElection()
	// A delayed response to the old request must not be counted, even if
	// the response happens to contain the current term.
	n.HandleRequestVoteReply("n3", old.Term, RequestVoteReply{Term: current.Term, VoteGranted: true})
	n.HandleRequestVoteReply("n3", current.Term, RequestVoteReply{Term: old.Term, VoteGranted: true})
	n.HandleRequestVoteReply("n4", current.Term, RequestVoteReply{Term: current.Term, VoteGranted: true})
	assertElectionStatus(t, n, current.Term, "Candidate", "")
	n.HandleRequestVoteReply("n2", current.Term, RequestVoteReply{Term: current.Term, VoteGranted: true})
	assertElectionStatus(t, n, current.Term, "Leader", "n1")
}

func TestHigherTermReplyStepsDownEvenForOldRequest(t *testing.T) {
	n := newElectionNode(t, "n2", "n3")
	old := n.StartElection()
	current := n.StartElection()
	n.HandleRequestVoteReply("n2", old.Term, RequestVoteReply{Term: current.Term + 1})
	assertElectionStatus(t, n, current.Term+1, "Follower", "")
	// Late votes cannot revive the abandoned election.
	n.HandleRequestVoteReply("n3", current.Term, RequestVoteReply{Term: current.Term, VoteGranted: true})
	assertElectionStatus(t, n, current.Term+1, "Follower", "")
	// Stepping down cleared the previous self-vote.
	reply := n.HandleRequestVote(RequestVoteArgs{Term: current.Term + 1, CandidateID: "n2"})
	if !reply.VoteGranted {
		t.Fatalf("vote was not cleared on higher term: %+v", reply)
	}
}

func TestLeaderStepsDownOnHigherTermVoteReply(t *testing.T) {
	n := newElectionNode(t, "n2", "n3")
	a := n.StartElection()
	n.HandleRequestVoteReply("n2", a.Term, RequestVoteReply{Term: a.Term, VoteGranted: true})
	assertElectionStatus(t, n, a.Term, "Leader", "n1")
	n.HandleRequestVoteReply("n3", a.Term, RequestVoteReply{Term: a.Term + 1})
	assertElectionStatus(t, n, a.Term+1, "Follower", "")
}

func TestSingleNodeElectsItself(t *testing.T) {
	n := newElectionNode(t)
	a := n.StartElection()
	assertElectionStatus(t, n, a.Term, "Leader", "n1")
}

type voteTransportFunc func(context.Context, string, RequestVoteArgs) (RequestVoteReply, error)

func (f voteTransportFunc) RequestVote(ctx context.Context, to string, args RequestVoteArgs) (RequestVoteReply, error) {
	return f(ctx, to, args)
}

// Election tests only provide vote responses; heartbeats are a separate step.
func (f voteTransportFunc) AppendEntries(context.Context, string, AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, errors.New("unexpected heartbeat in vote-only transport")
}

func TestRunElectionConnectsThreeNodes(t *testing.T) {
	tr := newTestTransport()

	n := newElectionNodeWithTransport(t, tr, "n2", "n3")
	for _, id := range []string{"n2", "n3"} {
		peer, err := NewNode(Config{ID: id}, tr)
		if err != nil {
			t.Fatal(err)
		}

		tr.Register(id, peer)
	}

	if err := n.RunElection(context.Background()); err != nil {
		t.Fatal(err)
	}

	assertElectionStatus(t, n, 1, "Leader", "n1")
}

func TestRunElectionContactsAllPeersDespiteTransportError(t *testing.T) {
	var (
		n  *Node
		mu sync.Mutex
	)

	calls := make(map[string]int)
	tr := voteTransportFunc(func(ctx context.Context, to string, args RequestVoteArgs) (RequestVoteReply, error) {
		// Callback into the node: transport must run without holding n.mu.
		status := n.Status()
		if status.Term != args.Term {
			t.Errorf("request term %d differs from node term %d", args.Term, status.Term)
		}

		mu.Lock()
		calls[to]++
		mu.Unlock()

		if to == "n2" {
			return RequestVoteReply{}, errors.New("peer unavailable")
		}

		return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
	})

	n = newElectionNodeWithTransport(t, tr, "n2", "n3")
	if err := n.RunElection(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(calls) != 2 || calls["n2"] != 1 || calls["n3"] != 1 {
		t.Fatalf("calls = %v, want one call per peer", calls)
	}

	assertElectionStatus(t, n, 1, "Leader", "n1")
}

func TestRunElectionWithoutQuorumRemainsCandidate(t *testing.T) {
	tr := voteTransportFunc(func(context.Context, string, RequestVoteArgs) (RequestVoteReply, error) {
		return RequestVoteReply{}, errors.New("peer unavailable")
	})

	n := newElectionNodeWithTransport(t, tr, "n2", "n3")
	if err := n.RunElection(context.Background()); err != nil {
		t.Fatal(err)
	}

	assertElectionStatus(t, n, 1, "Candidate", "")
}

func TestRunElectionAlreadyCanceledDoesNotStartRound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tr := voteTransportFunc(func(context.Context, string, RequestVoteArgs) (RequestVoteReply, error) {
		t.Error("canceled election must not send requests")
		return RequestVoteReply{}, nil
	})

	n := newElectionNodeWithTransport(t, tr, "n2", "n3")
	if err := n.RunElection(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	assertElectionStatus(t, n, 0, "Follower", "")
}

func newElectionNodeWithTransport(t *testing.T, tr Transport, peers ...string) *Node {
	t.Helper()

	n, err := NewNode(Config{ID: "n1", Peers: peers}, tr)
	if err != nil {
		t.Fatal(err)
	}

	return n
}

func TestRunElectionReportsCancellationDuringRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := voteTransportFunc(func(ctx context.Context, _ string, _ RequestVoteArgs) (RequestVoteReply, error) {
		cancel()
		return RequestVoteReply{}, ctx.Err()
	})

	n := newElectionNodeWithTransport(t, tr, "n2", "n3")
	if err := n.RunElection(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	assertElectionStatus(t, n, 1, "Candidate", "")
}

// Test-only routing avoids importing inmemory, which itself imports raft.
// Register all receivers before sending requests.
type testTransport struct{ receivers map[string]Receiver }

func newTestTransport() *testTransport                          { return &testTransport{receivers: make(map[string]Receiver)} }
func (tr *testTransport) Register(id string, receiver Receiver) { tr.receivers[id] = receiver }
func (tr *testTransport) RequestVote(ctx context.Context, to string, args RequestVoteArgs) (RequestVoteReply, error) {
	if err := ctx.Err(); err != nil {
		return RequestVoteReply{}, fmt.Errorf("test vote request: %w", err)
	}

	receiver, ok := tr.receivers[to]
	if !ok {
		return RequestVoteReply{}, errors.New("peer unavailable")
	}

	return receiver.HandleRequestVote(args), nil
}
func (tr *testTransport) AppendEntries(ctx context.Context, to string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	if err := ctx.Err(); err != nil {
		return AppendEntriesReply{}, fmt.Errorf("test heartbeat: %w", err)
	}

	receiver, ok := tr.receivers[to]
	if !ok {
		return AppendEntriesReply{}, errors.New("peer unavailable")
	}

	return receiver.HandleAppendEntries(args), nil
}

func TestRequestVoteLogFreshness(t *testing.T) {
	localLog := []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 2},
	}

	tests := []struct {
		name      string
		emptyLog  bool
		lastTerm  uint64
		lastIndex uint64
		wantVote  bool
	}{
		{"both empty", true, 0, 0, true},
		{"candidate has entries local log empty", true, 1, 1, true},
		{"candidate empty local log populated", false, 0, 0, false},
		{"same last term and index", false, 2, 3, true},
		{"same term shorter log", false, 2, 2, false},
		{"same term longer log", false, 2, 4, true},
		{"older last term despite longer log", false, 1, 10, false},
		{"newer last term despite shorter log", false, 3, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := NewNode(Config{ID: "node-1", Peers: []string{"node-2", "node-3"}}, nil)
			if err != nil {
				t.Fatal(err)
			}

			if !tt.emptyLog {
				node.persistentState.Log = append([]LogEntry(nil), localLog...)
			}

			reply := node.HandleRequestVote(RequestVoteArgs{
				Term:         4,
				CandidateID:  "node-2",
				LastLogTerm:  tt.lastTerm,
				LastLogIndex: tt.lastIndex,
			})
			if reply.Term != 4 || reply.VoteGranted != tt.wantVote {
				t.Fatalf("reply = %+v, want Term=4 VoteGranted=%v", reply, tt.wantVote)
			}

			if status := node.Status(); status.Term != 4 {
				t.Fatalf("term must advance even when candidate log is stale: %+v", status)
			}

			// A rejected candidate must not consume the vote for this term.
			if !tt.wantVote {
				retry := node.HandleRequestVote(RequestVoteArgs{
					Term: 4, CandidateID: "node-3", LastLogTerm: 3, LastLogIndex: 3,
				})
				if retry.Term != 4 || !retry.VoteGranted {
					t.Fatalf("fresh candidate should receive unspent vote: %+v", retry)
				}
			}
		})
	}
}

func deliverSplitVotes(
	t *testing.T, ctx context.Context, tr Transport, ids []string,
	nodes map[string]*Node, requests map[string]RequestVoteArgs,
) {
	t.Helper()

	for _, from := range ids {
		for _, to := range ids {
			if from == to {
				continue
			}

			reply, err := tr.RequestVote(ctx, to, requests[from])
			if err != nil {
				t.Fatal(err)
			}

			if reply.Term != 1 || reply.VoteGranted {
				t.Fatalf("%s voted for %s after voting for itself: %+v", to, from, reply)
			}

			nodes[from].HandleRequestVoteReply(to, requests[from].Term, reply)
		}
	}
}

func createElectionCluster(t *testing.T, tr *testTransport, ids []string) map[string]*Node {
	t.Helper()

	nodes := make(map[string]*Node)
	for _, id := range ids {
		peers := make([]string, 0, 2)

		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		n, err := NewNode(Config{ID: id, Peers: peers}, tr)
		if err != nil {
			t.Fatal(err)
		}

		nodes[id] = n
		tr.Register(id, n)
	}

	return nodes
}
