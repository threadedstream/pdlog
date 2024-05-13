package log

import (
	"time"

	"github.com/hashicorp/raft"
)

type Config struct {
	Raft struct {
		raft.Config
		StreamLayer  *StreamLayer
		Bootstrap    bool
		LeaseTimeout time.Duration
	}
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
