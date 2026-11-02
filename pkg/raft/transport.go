package raft

import "context"

type Transport interface {
	RequestVote(
		ctx context.Context,
		to string,
		args RequestVoteArgs,
	) (RequestVoteReply, error)
	AppendEntries(
		ctx context.Context,
		to string,
		args AppendEntriesArgs,
	) (AppendEntriesReply, error)
}

type Receiver interface {
	HandleRequestVote(args RequestVoteArgs) RequestVoteReply
	HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply
}
