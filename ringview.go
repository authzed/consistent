package consistent

import (
	"errors"
	"sync/atomic"

	"github.com/authzed/consistent/hashring"
)

// ErrNoRing means that no hashring exists for the target. gRPC builds
// balancers lazily, so before the first connection attempt there is no ring
// to read. A closed balancer also withdraws its ring.
var ErrNoRing = errors.New("no hashring available for target")

// RingView is a read-only view of the live hashring a balancer routes with.
//
// A view is a stable handle. It follows the balancer's ring across
// replacements (a ReplicationFactor change replaces the ring) and it shows
// membership changes as they happen. Queries through a view therefore agree
// with the picker's routing decisions. The one exception is a membership
// change that races the query: that can misplace a request's locality, never
// its delivery.
type RingView interface {
	// FindN returns the first n unique members of the ring for the given key,
	// exactly as the balancer's picker sees them. It returns ErrNoRing if no
	// ring exists yet for the target.
	FindN(key []byte, n uint8) ([]hashring.Member, error)

	// Members returns all members currently on the ring. It returns nil if
	// no ring exists yet for the target.
	Members() []hashring.Member
}

// ringSlot is the shared cell that connects a balancer to its RingView
// readers: the balancer publishes its live ring here and the readers load
// it. The pointer is nil until the balancer creates its first ring. The
// balancer swaps the pointer when it replaces the ring and clears the
// pointer on Close.
type ringSlot struct {
	ring atomic.Pointer[hashring.Ring]
}

var _ RingView = (*ringSlot)(nil)

func (s *ringSlot) FindN(key []byte, n uint8) ([]hashring.Member, error) {
	r := s.ring.Load()
	if r == nil {
		return nil, ErrNoRing
	}
	return r.FindN(key, n)
}

func (s *ringSlot) Members() []hashring.Member {
	r := s.ring.Load()
	if r == nil {
		return nil
	}
	return r.Members()
}
