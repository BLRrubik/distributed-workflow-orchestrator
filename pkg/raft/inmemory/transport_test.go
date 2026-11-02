package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blrrubik/distributed-workflow-orchestrator/pkg/raft"
)

// slowReplyTransport delivers requests normally, but withholds n3's replies
// until the RPC context expires. All three nodes can still receive heartbeats.
type slowReplyTransport struct {
	base              *MemoryTransport
	voteTimeouts      atomic.Int32
	heartbeatTimeouts atomic.Int32
}

func (tr *slowReplyTransport) RequestVote(ctx context.Context, to string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	reply, err := tr.base.RequestVote(ctx, to, args)
	if err != nil || to != "n3" {
		return reply, err
	}

	<-ctx.Done()

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		tr.voteTimeouts.Add(1)
	}

	return raft.RequestVoteReply{}, fmt.Errorf("slow vote reply: %w", ctx.Err())
}

func (tr *slowReplyTransport) AppendEntries(ctx context.Context, to string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	reply, err := tr.base.AppendEntries(ctx, to, args)
	if err != nil || to != "n3" {
		return reply, err
	}

	<-ctx.Done()

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		tr.heartbeatTimeouts.Add(1)
	}

	return raft.AppendEntriesReply{}, fmt.Errorf("slow heartbeat reply: %w", ctx.Err())
}

func TestRunningClusterKeepsLeaderWithSlowPeerReplies(t *testing.T) {
	tr := &slowReplyTransport{base: NewMemoryTransport()}
	nodes := make(map[string]*raft.Node)

	ids := []string{"n1", "n2", "n3"}
	for _, id := range ids {
		peers := make([]string, 0, 2)

		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		n, err := raft.NewNode(raft.Config{ID: id, Peers: peers, RPCTimeout: 30 * time.Millisecond}, tr)
		if err != nil {
			t.Fatal(err)
		}

		nodes[id] = n
		tr.base.Register(id, n)
		t.Cleanup(func() { stopClusterNode(t, n) })
	}
	// Start the healthy majority first, so the selected leader must exercise
	// the slow response path to n3. The third node then runs normally too.
	for _, id := range []string{"n1", "n2"} {
		if err := nodes[id].Start(); err != nil {
			t.Fatal(err)
		}
	}

	leader, term := waitClusterLeader(t, nodes)
	if leader == "n3" {
		t.Fatal("node not started unexpectedly became leader")
	}

	if err := nodes["n3"].Start(); err != nil {
		t.Fatal(err)
	}

	timer := time.NewTimer(700 * time.Millisecond)
	defer timer.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

observe:
	for {
		select {
		case <-timer.C:
			break observe
		case <-ticker.C:
			id, gotTerm, ok := convergedCluster(nodes)
			if !ok || id != leader || gotTerm != term {
				t.Fatalf("slow replies disrupted cluster: leader=%s term=%d converged=%v, want %s term=%d", id, gotTerm, ok, leader, term)
			}
		}
	}

	if tr.voteTimeouts.Load() == 0 {
		t.Error("no vote RPC deadline was exercised")
	}

	if got := tr.heartbeatTimeouts.Load(); got < 3 {
		t.Errorf("only %d heartbeat timeouts; expected repeated rounds", got)
	}
}

// partitionNetwork is a test-only fault injector around the real transport.
// Each endpoint knows its sender; isolation blocks both incoming and outgoing RPCs.
type partitionNetwork struct {
	base     *MemoryTransport
	mu       sync.RWMutex
	isolated string
}

type partitionEndpoint struct {
	network *partitionNetwork
	from    string
}

func (n *partitionNetwork) isolate(id string) {
	n.mu.Lock()
	n.isolated = id // Empty ID restores all links.
	n.mu.Unlock()
}

func (n *partitionNetwork) blocked(from, to string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.isolated != "" && (from == n.isolated || to == n.isolated)
}

func (e partitionEndpoint) RequestVote(ctx context.Context, to string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	if err := ctx.Err(); err != nil {
		return raft.RequestVoteReply{}, fmt.Errorf("partition vote canceled: %w", err)
	}

	if e.network.blocked(e.from, to) {
		return raft.RequestVoteReply{}, errors.New("link partitioned")
	}

	return e.network.base.RequestVote(ctx, to, args)
}

func (e partitionEndpoint) AppendEntries(ctx context.Context, to string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	if err := ctx.Err(); err != nil {
		return raft.AppendEntriesReply{}, fmt.Errorf("partition heartbeat canceled: %w", err)
	}

	if e.network.blocked(e.from, to) {
		return raft.AppendEntriesReply{}, errors.New("link partitioned")
	}

	return e.network.base.AppendEntries(ctx, to, args)
}

func TestPartitionNetworkBlocksBothDirections(t *testing.T) {
	network := &partitionNetwork{base: NewMemoryTransport()}

	for _, id := range []string{"n1", "n2", "n3"} {
		n, err := raft.NewNode(raft.Config{ID: id}, nil)
		if err != nil {
			t.Fatal(err)
		}

		network.base.Register(id, n)
	}

	network.isolate("n1")

	for _, link := range [][2]string{{"n1", "n2"}, {"n2", "n1"}, {"n1", "n3"}, {"n3", "n1"}} {
		e := partitionEndpoint{network: network, from: link[0]}
		if _, err := e.RequestVote(context.Background(), link[1], raft.RequestVoteArgs{Term: 1, CandidateID: link[0]}); err == nil {
			t.Fatalf("vote crossed blocked link %v", link)
		}

		if _, err := e.AppendEntries(context.Background(), link[1], raft.AppendEntriesArgs{Term: 1, LeaderID: link[0]}); err == nil {
			t.Fatalf("heartbeat crossed blocked link %v", link)
		}
	}

	e := partitionEndpoint{network: network, from: "n2"}
	if _, err := e.AppendEntries(context.Background(), "n3", raft.AppendEntriesArgs{Term: 1, LeaderID: "n2"}); err != nil {
		t.Fatal(err)
	}

	network.isolate("")

	if _, err := e.AppendEntries(context.Background(), "n1", raft.AppendEntriesArgs{Term: 1, LeaderID: "n2"}); err != nil {
		t.Fatal(err)
	}
}

func TestThreeNodeClusterPartitionAndRecovery(t *testing.T) {
	network := &partitionNetwork{base: NewMemoryTransport()}
	nodes := make(map[string]*raft.Node)

	ids := []string{"n1", "n2", "n3"}
	for _, id := range ids {
		peers := make([]string, 0, 2)

		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		n, err := raft.NewNode(raft.Config{ID: id, Peers: peers}, partitionEndpoint{network: network, from: id})
		if err != nil {
			t.Fatal(err)
		}

		nodes[id] = n
		network.base.Register(id, n)
		t.Cleanup(func() { stopClusterNode(t, n) })
	}

	for _, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
	}

	oldLeader, oldTerm := waitClusterLeader(t, nodes)
	network.isolate(oldLeader)

	majority := make(map[string]*raft.Node)

	for id, n := range nodes {
		if id != oldLeader {
			majority[id] = n
		}
	}

	newLeader, newTerm := waitClusterLeader(t, majority)
	if newTerm <= oldTerm {
		t.Fatalf("majority term=%d, want > %d", newTerm, oldTerm)
	}
	// The old leader may retain its role, but must not share a term with the new one.
	old := nodes[oldLeader].Status()
	if old.State == "Leader" && old.Term == newTerm {
		t.Fatal("two leaders in the same term")
	}

	network.isolate("")

	recoveredLeader, recoveredTerm := waitClusterLeader(t, nodes)
	if recoveredLeader != newLeader || recoveredTerm != newTerm {
		t.Fatalf("unexpected recovery: leader=%s term=%d, want %s term=%d", recoveredLeader, recoveredTerm, newLeader, newTerm)
	}

	if got := nodes[oldLeader].Status(); got.State != "Follower" || got.LeaderID != newLeader {
		t.Fatalf("old leader failed to step down: %+v", got)
	}
}

func stopClusterNode(t *testing.T, n *raft.Node) {
	t.Helper()

	done := make(chan struct{})

	go func() { n.Stop(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not finish within 2 seconds")
	}
}

// A converged cluster has one leader and followers that know its ID and term.
func convergedCluster(nodes map[string]*raft.Node) (string, uint64, bool) {
	statuses := make([]raft.NodeStatus, 0, len(nodes))

	var (
		leader string
		term   uint64
	)

	for id, n := range nodes {
		s := n.Status()

		statuses = append(statuses, s)

		if s.State == "Leader" {
			if leader != "" {
				return "", 0, false
			}

			leader, term = id, s.Term
		}
	}

	if leader == "" {
		return "", 0, false
	}

	for _, s := range statuses {
		if s.LeaderID != leader || s.Term != term || (s.ID != leader && s.State != "Follower") {
			return "", 0, false
		}
	}

	return leader, term, true
}

func waitClusterLeader(t *testing.T, nodes map[string]*raft.Node) (string, uint64) {
	t.Helper()

	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if id, term, ok := convergedCluster(nodes); ok {
			return id, term
		}

		select {
		case <-ticker.C:
		case <-timer.C:
			for id, n := range nodes {
				t.Logf("%s: %+v", id, n.Status())
			}

			t.Fatal("cluster did not converge within 3 seconds")
		}
	}
}

func TestThreeNodeClusterElectionAndFailover(t *testing.T) {
	tr := NewMemoryTransport()
	nodes := make(map[string]*raft.Node)

	ids := []string{"n1", "n2", "n3"}
	for _, id := range ids {
		peers := make([]string, 0, 2)

		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		n, err := raft.NewNode(raft.Config{ID: id, Peers: peers}, tr)
		if err != nil {
			t.Fatal(err)
		}

		nodes[id] = n
		tr.Register(id, n)
		t.Cleanup(func() { stopClusterNode(t, n) })
	}

	for _, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
	}

	leader, term := waitClusterLeader(t, nodes)
	// Several election timeouts pass; heartbeats must keep the term stable.
	deadline := time.NewTimer(500 * time.Millisecond)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

stable:
	for {
		select {
		case <-deadline.C:
			break stable
		case <-ticker.C:
			id, gotTerm, ok := convergedCluster(nodes)
			if !ok || id != leader || gotTerm != term {
				deadline.Stop()
				t.Fatalf("stable cluster changed: leader=%s term=%d converged=%v", id, gotTerm, ok)
			}
		}
	}

	tr.Unregister(leader)
	stopClusterNode(t, nodes[leader])
	delete(nodes, leader)

	newLeader, newTerm := waitClusterLeader(t, nodes)
	if newTerm <= term {
		t.Fatalf("term after failover = %d, want > %d", newTerm, term)
	}
	// Disconnect the new leader too: one remaining node is NOT a one-node cluster.
	tr.Unregister(newLeader)
	stopClusterNode(t, nodes[newLeader])
	delete(nodes, newLeader)

	for _, remaining := range nodes {
		assertMinorityCannotElect(t, remaining)
	}
}

func newElectionNode(t *testing.T, peers ...string) *raft.Node {
	t.Helper()

	n, err := raft.NewNode(raft.Config{ID: "n1", Peers: peers}, nil)
	if err != nil {
		t.Fatal(err)
	}

	return n
}

func assertElectionStatus(t *testing.T, n *raft.Node, term uint64, role, leader string) {
	t.Helper()

	want := raft.NodeStatus{ID: "n1", Term: term, State: role, LeaderID: leader}
	if got := n.Status(); got != want {
		t.Fatalf("Status() = %+v, want %+v", got, want)
	}
}

func TestMemoryTransportDeliversVotesToSelectedNode(t *testing.T) {
	n1 := newElectionNode(t, "n2", "n3")

	n2, err := raft.NewNode(raft.Config{ID: "n2", Peers: []string{"n1", "n3"}}, nil)
	if err != nil {
		t.Fatal(err)
	}

	n3, err := raft.NewNode(raft.Config{ID: "n3", Peers: []string{"n1", "n2"}}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tr := NewMemoryTransport()
	tr.Register("n2", n2)
	tr.Register("n3", n3)

	var _ raft.Transport = tr

	args := n1.StartElection()

	reply, err := tr.RequestVote(context.Background(), "n2", args)
	if err != nil {
		t.Fatal(err)
	}

	if reply.Term != args.Term || !reply.VoteGranted {
		t.Fatalf("unexpected response: %+v", reply)
	}

	if n2.Status().Term != args.Term || n3.Status().Term != 0 {
		t.Fatal("request must reach n2 only")
	}

	n1.HandleRequestVoteReply("n2", args.Term, reply)
	assertElectionStatus(t, n1, args.Term, "Leader", "n1")
}

func TestMemoryTransportUnknownPeerReturnsError(t *testing.T) {
	tr := NewMemoryTransport()

	_, err := tr.RequestVote(context.Background(), "missing", raft.RequestVoteArgs{Term: 1, CandidateID: "n1"})
	if err == nil {
		t.Fatal("unknown destination must return an error")
	}
}

func TestMemoryTransportCanceledContextDoesNotDeliver(t *testing.T) {
	n := newElectionNode(t, "n2", "n3")
	tr := NewMemoryTransport()
	tr.Register("n1", n)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := tr.RequestVote(ctx, "n1", raft.RequestVoteArgs{Term: 10, CandidateID: "n2"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	assertElectionStatus(t, n, 0, "Follower", "")
}

func TestMemoryTransportDeliversHeartbeatToSelectedNode(t *testing.T) {
	n1 := newElectionNode(t, "n2", "n3")

	n2, err := raft.NewNode(raft.Config{ID: "n2"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tr := NewMemoryTransport()
	tr.Register("n1", n1)
	tr.Register("n2", n2)

	reply, err := tr.AppendEntries(context.Background(), "n1", raft.AppendEntriesArgs{Term: 3, LeaderID: "n3"})
	if err != nil {
		t.Fatal(err)
	}

	if reply != (raft.AppendEntriesReply{Term: 3, Success: true}) {
		t.Fatalf("unexpected reply: %+v", reply)
	}

	assertElectionStatus(t, n1, 3, "Follower", "n3")

	if got := n2.Status(); got.Term != 0 || got.LeaderID != "" {
		t.Fatalf("heartbeat reached wrong node: %+v", got)
	}
	// A protocol rejection is a normal response, not a transport error.
	reply, err = tr.AppendEntries(context.Background(), "n1", raft.AppendEntriesArgs{Term: 2, LeaderID: "n2"})
	if err != nil || reply != (raft.AppendEntriesReply{Term: 3, Success: false}) {
		t.Fatalf("reply=%+v error=%v, want rejection at term 3 without transport error", reply, err)
	}

	assertElectionStatus(t, n1, 3, "Follower", "n3")
}

func TestMemoryTransportHeartbeatUnknownPeer(t *testing.T) {
	tr := NewMemoryTransport()

	_, err := tr.AppendEntries(context.Background(), "missing", raft.AppendEntriesArgs{Term: 1, LeaderID: "n2"})
	if err == nil {
		t.Fatal("unknown destination must return an error")
	}
}

func TestMemoryTransportCanceledHeartbeatDoesNotDeliver(t *testing.T) {
	n := newElectionNode(t, "n2", "n3")
	tr := NewMemoryTransport()
	tr.Register("n1", n)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := tr.AppendEntries(ctx, "n1", raft.AppendEntriesArgs{Term: 10, LeaderID: "n2"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context.Canceled", err)
	}

	assertElectionStatus(t, n, 0, "Follower", "")
}

func assertMinorityCannotElect(t *testing.T, remaining *raft.Node) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	before := remaining.Status().Term

	until := time.NewTimer(700 * time.Millisecond)
	defer until.Stop()

minority:
	for {
		select {
		case <-until.C:
			break minority
		case <-ticker.C:
			if remaining.Status().State == "Leader" {
				t.Fatal("minority elected itself")
			}
		}
	}

	if remaining.Status().Term <= before {
		t.Fatal("remaining node did not attempt a new election")
	}
}
