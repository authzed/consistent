package rendezvous

import (
	"encoding/binary"
	"math/rand"
	"strconv"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"

	"github.com/authzed/consistent/hashring"
)

type testNode string

func (tn testNode) Key() string { return string(tn) }

func nodes(n int) []testNode {
	out := make([]testNode, n)
	for i := range out {
		out[i] = testNode("node-" + strconv.Itoa(i))
	}
	return out
}

func keys(n int, seed int64) [][]byte {
	r := rand.New(rand.NewSource(seed))
	out := make([][]byte, n)
	for i := range out {
		out[i] = binary.LittleEndian.AppendUint64(nil, r.Uint64())
	}
	return out
}

func owner(t testing.TB, s *Set, key []byte) string {
	found, err := s.FindN(key, 1)
	require.NoError(t, err)
	return found[0].Key()
}

func TestAddRemove(t *testing.T) {
	s := New(xxhash.Sum64)

	_, err := s.FindN([]byte("key"), 1)
	require.ErrorIs(t, err, ErrNotEnoughMembers)

	require.NoError(t, s.Add(testNode("a")))
	require.ErrorIs(t, s.Add(testNode("a")), ErrMemberAlreadyExists)
	require.NoError(t, s.Add(testNode("b")))
	require.Len(t, s.Members(), 2)

	found, err := s.FindN([]byte("key"), 0)
	require.NoError(t, err)
	require.Empty(t, found)

	_, err = s.FindN([]byte("key"), 3)
	require.ErrorIs(t, err, ErrNotEnoughMembers)

	require.ErrorIs(t, s.Remove(testNode("c")), ErrMemberNotFound)
	require.NoError(t, s.Remove(testNode("a")))
	require.Equal(t, "b", owner(t, s, []byte("key")))
	require.NoError(t, s.Remove(testNode("b")))
	require.Empty(t, s.Members())
}

func TestFindNIsOrderedAndDistinct(t *testing.T) {
	s := New(xxhash.Sum64)
	for _, n := range nodes(10) {
		require.NoError(t, s.Add(n))
	}

	for _, key := range keys(1000, 1) {
		all, err := s.FindN(key, 10)
		require.NoError(t, err)

		seen := map[string]struct{}{}
		for _, m := range all {
			seen[m.Key()] = struct{}{}
		}
		require.Len(t, seen, 10)

		// Every prefix of the full ranking must equal FindN with that count.
		for num := 1; num <= 10; num++ {
			prefix, err := s.FindN(key, uint8(num))
			require.NoError(t, err)
			require.Equal(t, all[:num], prefix)
		}
	}
}

func TestInsertionOrderIndependence(t *testing.T) {
	ns := nodes(20)
	a, b := New(xxhash.Sum64), New(xxhash.Sum64)
	for _, n := range ns {
		require.NoError(t, a.Add(n))
	}
	for _, i := range rand.New(rand.NewSource(2)).Perm(len(ns)) {
		require.NoError(t, b.Add(ns[i]))
	}

	for _, key := range keys(1000, 3) {
		fa, err := a.FindN(key, 3)
		require.NoError(t, err)
		fb, err := b.FindN(key, 3)
		require.NoError(t, err)
		require.Equal(t, fa, fb)
	}
}

// ringLike is the API shared by hashring.Ring and Set.
type ringLike interface {
	Add(hashring.Member) error
	Remove(hashring.Member) error
	FindN([]byte, uint8) ([]hashring.Member, error)
}

// TestMinimalDisruption checks that removing a member only moves the keys it
// owned, and adding a member only moves keys onto that member.
func TestMinimalDisruption(t *testing.T) {
	impls := map[string]func() ringLike{
		"hrw":       func() ringLike { return New(xxhash.Sum64) },
		"ring-r100": func() ringLike { return hashring.MustNew(xxhash.Sum64, 100) },
	}
	ks := keys(10_000, 4)

	for name, newImpl := range impls {
		t.Run(name, func(t *testing.T) {
			s := newImpl()
			ns := nodes(10)
			for _, n := range ns {
				require.NoError(t, s.Add(n))
			}
			before := owners(t, s, ks)

			require.NoError(t, s.Remove(ns[3]))
			afterRemove := owners(t, s, ks)
			for i := range ks {
				if before[i] != ns[3].Key() {
					require.Equal(t, before[i], afterRemove[i])
				}
			}

			require.NoError(t, s.Add(testNode("new")))
			afterAdd := owners(t, s, ks)
			for i := range ks {
				if afterAdd[i] != "new" {
					require.Equal(t, afterRemove[i], afterAdd[i])
				}
			}
		})
	}
}

func owners(t testing.TB, s ringLike, ks [][]byte) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		found, err := s.FindN(k, 1)
		require.NoError(t, err)
		out[i] = found[0].Key()
	}
	return out
}
