package raft

import (
	"context"
	"fmt"
	"sync"
)

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.persistentState.CurrentTerm {
		return AppendEntriesReply{
			Term: n.persistentState.CurrentTerm,
		}
	}

	if args.Term > n.persistentState.CurrentTerm {
		n.changeToFollowerLocked(args.Term)
	}

	n.state = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()

	return AppendEntriesReply{
		Term:    n.persistentState.CurrentTerm,
		Success: true,
	}
}

func (n *Node) SendHeartbeats(ctx context.Context) error {
	if ctx.Err() != nil {
		return fmt.Errorf("context error: %w", ctx.Err())
	}

	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()

		return nil
	}

	args := AppendEntriesArgs{
		Term:     n.persistentState.CurrentTerm,
		LeaderID: n.leaderID,
	}

	n.mu.Unlock()

	wg := sync.WaitGroup{}
	for peer := range n.peers {
		wg.Add(1)

		go func(peer string) {
			defer wg.Done()

			rpcCtx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
			defer cancel()

			res, err := n.transport.AppendEntries(rpcCtx, peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if res.Term > n.persistentState.CurrentTerm {
				n.changeToFollowerLocked(res.Term)
			}
		}(peer)
	}

	wg.Wait()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("raft operation canceled: %w", err)
	}

	return nil
}
