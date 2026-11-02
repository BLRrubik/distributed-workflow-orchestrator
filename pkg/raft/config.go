package raft

import (
	"errors"
	"time"
)

const (
	defaultHeartbeatTimeout = 50 * time.Millisecond
	defaultRPCTimeout       = 50 * time.Millisecond
)

type Config struct {
	ID    string
	Peers []string

	HeartbeatTimeout time.Duration
	RPCTimeout       time.Duration
}

func (c *Config) Validate() error {
	if len(c.ID) == 0 {
		return errors.New("ID is required")
	}

	seenPeers := make(map[string]struct{}, len(c.Peers))

	for _, id := range c.Peers {
		if len(id) == 0 {
			return errors.New("peer ID is required")
		}

		if _, ok := seenPeers[id]; ok {
			return errors.New("peer ID is repeated")
		}

		seenPeers[id] = struct{}{}

		if id == c.ID {
			return errors.New("peer ID is the same")
		}
	}

	if c.HeartbeatTimeout < 0 {
		return errors.New("heartbeat timeout is negative")
	}

	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = defaultHeartbeatTimeout
	}

	if c.RPCTimeout < 0 {
		return errors.New("rpc timeout is negative")
	}

	if c.RPCTimeout == 0 {
		c.RPCTimeout = defaultRPCTimeout
	}

	return nil
}
