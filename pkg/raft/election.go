package raft

import (
	"context"
	"fmt"
	"sync"
)

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.persistentState.CurrentTerm {
		return RequestVoteReply{
			Term: n.persistentState.CurrentTerm,
		}
	}

	if args.Term > n.persistentState.CurrentTerm {
		n.changeToFollowerLocked(args.Term)
	}

	voteGranted := n.applyVoteLocked(args)

	if voteGranted {
		n.resetElectionTimerLocked()
	}

	return RequestVoteReply{
		Term:        n.persistentState.CurrentTerm,
		VoteGranted: voteGranted,
	}
}

func (n *Node) StartElection() RequestVoteArgs {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.votes = make(map[string]struct{})
	n.votes[n.cfg.ID] = struct{}{}

	n.persistentState.CurrentTerm++
	n.state = Candidate
	n.leaderID = ""
	n.persistentState.VotedFor = n.cfg.ID
	n.resetElectionTimerLocked()

	lastLog := n.getLastLogLocked()

	if len(n.votes) >= n.getQuorumLocked() {
		n.changeToLeaderLocked()
	}

	return RequestVoteArgs{
		Term:         n.persistentState.CurrentTerm,
		CandidateID:  n.cfg.ID,
		LastLogTerm:  lastLog.Term,
		LastLogIndex: lastLog.Index,
	}
}

func (n *Node) RunElection(ctx context.Context) error {
	if ctx.Err() != nil {
		return fmt.Errorf("context error: %w", ctx.Err())
	}

	args := n.StartElection()

	wg := sync.WaitGroup{}
	for peer := range n.peers {
		wg.Add(1)

		go func(peer string) {
			defer wg.Done()

			rpcCtx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
			defer cancel()

			res, err := n.transport.RequestVote(rpcCtx, peer, args)
			if err != nil {
				return
			}

			n.HandleRequestVoteReply(peer, args.Term, res)
		}(peer)
	}

	wg.Wait()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("raft operation canceled: %w", err)
	}

	return nil
}

func (n *Node) HandleRequestVoteReply(
	peerID string,
	electionTerm uint64,
	reply RequestVoteReply,
) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.peers[peerID]; !ok {
		return
	}

	if reply.Term > n.persistentState.CurrentTerm {
		n.changeToFollowerLocked(reply.Term)

		return
	}

	if n.state != Candidate {
		return
	}

	if n.persistentState.CurrentTerm != electionTerm || n.persistentState.CurrentTerm != reply.Term {
		return
	}

	if _, ok := n.votes[peerID]; ok {
		return
	}

	if reply.VoteGranted {
		n.votes[peerID] = struct{}{}
	}

	if len(n.votes) >= n.getQuorumLocked() {
		n.changeToLeaderLocked()
	}
}

func (n *Node) applyVoteLocked(args RequestVoteArgs) bool {
	if !n.isJournalActual(args.LastLogTerm, args.LastLogIndex) {
		return false
	}

	if len(n.persistentState.VotedFor) == 0 {
		n.persistentState.VotedFor = args.CandidateID

		return true
	}

	return args.CandidateID == n.persistentState.VotedFor
}

func (n *Node) isJournalActual(lastLogTerm, lastLogIndex uint64) bool {
	lastLog := n.getLastLogLocked()

	return lastLogTerm > lastLog.Term ||
		(lastLogTerm == lastLog.Term && lastLogIndex >= lastLog.Index)
}
