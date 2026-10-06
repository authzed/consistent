// Package rendezvous implements a thread-safe rendezvous (highest random
// weight) hashing member set with a pluggable hashing algorithm.
//
// It exposes the same API shape as the hashring package so that the two can
// be used interchangeably.
package rendezvous

import (
	"slices"
	"strings"
	"sync"

	"github.com/authzed/consistent/hashring"
)

// These are the same values as their hashring counterparts, so callers can
// check errors identically for either implementation.
var (
	ErrMemberAlreadyExists = hashring.ErrMemberAlreadyExists
	ErrMemberNotFound      = hashring.ErrMemberNotFound
	ErrNotEnoughMembers    = hashring.ErrNotEnoughMembers
)

// Set provides a thread-safe rendezvous hashing implementation.
//
// Each lookup hashes the key once and then scores every member by mixing the
// key hash with the member's precomputed hash. The members with the highest
// scores are selected. Lookups are O(n) in the number of members, but each
// score is a handful of arithmetic instructions over a small, contiguous
// slice, which is faster than a vnode ring for the member counts typical of
// a gRPC backend set.
type Set struct {
	hashfn hashring.HashFunc

	sync.RWMutex
	// members is kept sorted by key so that ties in score are broken
	// deterministically, independent of insertion order.
	members []member
	// hashes holds the hash of members[i] at index i. It is kept separate so
	// the scoring loop reads a dense slice.
	hashes []uint64
}

type member struct {
	key    string
	member hashring.Member
}

// New allocates a Set with the specified hash function.
func New(hashfn hashring.HashFunc) *Set {
	return &Set{hashfn: hashfn}
}

func cmpKey(m member, key string) int {
	return strings.Compare(m.key, key)
}

// Add inserts a member into the set.
//
// If a member with the same key is already in the set, ErrMemberAlreadyExists
// is returned.
func (s *Set) Add(m hashring.Member) error {
	key := m.Key()
	hash := s.hashfn([]byte(key))

	s.Lock()
	defer s.Unlock()

	i, found := slices.BinarySearchFunc(s.members, key, cmpKey)
	if found {
		return ErrMemberAlreadyExists
	}
	s.members = slices.Insert(s.members, i, member{key, m})
	s.hashes = slices.Insert(s.hashes, i, hash)
	return nil
}

// Remove removes the specified member from the set.
//
// If no member can be found, ErrMemberNotFound is returned.
func (s *Set) Remove(m hashring.Member) error {
	key := m.Key()

	s.Lock()
	defer s.Unlock()

	i, found := slices.BinarySearchFunc(s.members, key, cmpKey)
	if !found {
		return ErrMemberNotFound
	}
	s.members = slices.Delete(s.members, i, i+1)
	s.hashes = slices.Delete(s.hashes, i, i+1)
	return nil
}

// FindN returns the N members with the highest scores for the specified key,
// ordered from highest to lowest score.
//
// If there are not enough members to satisfy the request, ErrNotEnoughMembers
// is returned.
func (s *Set) FindN(key []byte, num uint8) ([]hashring.Member, error) {
	s.RLock()
	defer s.RUnlock()

	if int(num) > len(s.members) {
		return nil, ErrNotEnoughMembers
	}
	if num == 0 {
		return []hashring.Member{}, nil
	}

	keyHash := s.hashfn(key)

	if num == 1 {
		best, bestScore := 0, combineHashes(keyHash, s.hashes[0])
		for i := 1; i < len(s.hashes); i++ {
			// Strict comparison: on a tie, the member with the lower key wins.
			if sc := combineHashes(keyHash, s.hashes[i]); sc > bestScore {
				best, bestScore = i, sc
			}
		}
		return []hashring.Member{s.members[best].member}, nil
	}

	// Maintain the top num candidates, sorted descending by score, with an
	// insertion sort. num is small, so this beats a heap.
	type candidate struct {
		score uint64
		index int
	}
	// A variable-size make stays on the stack only up to 32 bytes (two
	// candidates), so use a fixed buffer for common spreads.
	var buf [8]candidate
	top := buf[:0]
	if int(num) > len(buf) {
		top = make([]candidate, 0, num)
	}
	for hashIndex, nodeHash := range s.hashes {
		sc := combineHashes(keyHash, nodeHash)
		if len(top) == int(num) && sc <= top[len(top)-1].score {
			continue
		}
		if len(top) < int(num) {
			top = append(top, candidate{})
		}
		// Strict comparison: on a tie, the member with the lower key ranks
		// first.
		pos := len(top) - 1
		for pos > 0 && top[pos-1].score < sc {
			top[pos] = top[pos-1]
			pos--
		}
		top[pos] = candidate{sc, hashIndex}
	}

	found := make([]hashring.Member, len(top))
	for i, c := range top {
		found[i] = s.members[c.index].member
	}
	return found, nil
}

// Members enumerates the full set of members.
func (s *Set) Members() []hashring.Member {
	s.RLock()
	defer s.RUnlock()

	membersCopy := make([]hashring.Member, 0, len(s.members))
	for _, m := range s.members {
		membersCopy = append(membersCopy, m.member)
	}
	return membersCopy
}

// combineHashes combines a key hash and a member hash into the member's weight for
// that key, resulting in a value that can then be sorted for target selection.
//
// The idea is that the resulting value should be relatively randomly and uniformly
// distributed; a naive implementation would use xxhash(old_hash || new_hash) or
// something like that. This function achieves similar results through bitshifting
// and multiplication with some magic constants, referencing the splitmix64 finalizer.
// The method was empirically determined to do a good job of distributing the resulting
// values, and it being bitshifts and multiplications means that it's a good deal faster
// than hashing. Think the fast inverse square root implementation.
func combineHashes(keyHash, memberHash uint64) uint64 {
	x := keyHash ^ memberHash
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
