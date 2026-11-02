package raft

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

const (
	electionLowerTimeout = 150 * time.Millisecond
	electionUpperTimeout = 300 * time.Millisecond
)

func (n *Node) Tick(ctx context.Context, elapsed time.Duration) error {
	if ctx.Err() != nil {
		return fmt.Errorf("tick canceled: %w", ctx.Err())
	}

	var (
		needHeartbeat bool
		needElection  bool
	)

	n.mu.Lock()

	if n.state == Leader {
		n.heartbeatElapsed += elapsed

		if n.heartbeatElapsed >= n.cfg.HeartbeatTimeout {
			n.resetHeartbeatTimerLocked()

			needHeartbeat = true
		}
	} else {
		n.electionElapsed += elapsed

		if n.electionElapsed >= n.electionTimeout {
			needElection = true
		}
	}

	n.mu.Unlock()

	if needElection {
		return n.RunElection(ctx)
	}

	if needHeartbeat {
		return n.SendHeartbeats(ctx)
	}

	return nil
}

func (n *Node) resetElectionTimerLocked() {
	// Election jitter does not require cryptographic randomness.
	//nolint:gosec // G404: randomized scheduling, not security-sensitive data.
	n.electionTimeout = time.Duration(rand.Int63n(int64(electionUpperTimeout-electionLowerTimeout))) + electionLowerTimeout
	n.electionElapsed = 0
}

func (n *Node) resetHeartbeatTimerLocked() {
	n.heartbeatElapsed = 0
}
