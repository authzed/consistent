package hashring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

type testNode struct {
	nodeKeyAndValue string
	addNodeError    error
}

func (tn testNode) Key() string {
	return tn.nodeKeyAndValue
}

func TestHashring(t *testing.T) {
	testCases := []struct {
		replicationFactor uint16
		nodes             []testNode
	}{
		{1, []testNode{}},
		{1, []testNode{{"key1", nil}}},
		{1, []testNode{{"key1", nil}, {"key2", nil}}},
		{20, []testNode{{"key1", nil}}},
		{20, []testNode{{"key1", nil}, {"key2", nil}}},
		{20, []testNode{{"key1", nil}, {"key1", ErrMemberAlreadyExists}}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(strconv.Itoa(int(tc.replicationFactor)), func(t *testing.T) {
			ring, err := New(xxhash.Sum64, tc.replicationFactor)
			require.NoError(t, err)

			require.NotNil(t, ring.hashfn)
			require.Equal(t, tc.replicationFactor, ring.replicationFactor)
			require.Len(t, ring.virtualNodes, 0)
			require.Len(t, ring.nodes, 0)

			successfulNodes := map[string]struct{}{}
			for _, testNodeInfo := range tc.nodes {
				err := ring.Add(testNodeInfo)
				require.Equal(t, testNodeInfo.addNodeError, err)

				if err == nil {
					successfulNodes[testNodeInfo.nodeKeyAndValue] = struct{}{}
				}

				require.Len(t, ring.virtualNodes, len(successfulNodes)*int(tc.replicationFactor))
				require.Len(t, ring.nodes, len(successfulNodes))

				// Try the find function
				if len(successfulNodes) > 0 {
					found, err := ring.FindN([]byte("key1"), 1)
					require.NoError(t, err)
					require.Len(t, found, 1)
					require.Contains(t, successfulNodes, found[0].Key())
				}

				checkAllFound := map[string]struct{}{}
				for k, v := range successfulNodes {
					checkAllFound[k] = v
				}
				allFound, err := ring.FindN([]byte("key1"), uint8(len(successfulNodes)))
				require.NoError(t, err)
				require.Len(t, allFound, len(successfulNodes))

				for _, found := range allFound {
					require.Contains(t, checkAllFound, found.Key())
					delete(checkAllFound, found.Key())
				}

				require.Empty(t, checkAllFound)

				// Ask for more nodes than exist
				_, err = ring.FindN([]byte("1"), uint8(len(successfulNodes)+1))
				require.Equal(t, ErrNotEnoughMembers, err)
			}

			// Build a consistent hash that adds the nodes in reverse order
			reverseRing, err := New(xxhash.Sum64, tc.replicationFactor)
			require.NoError(t, err)

			for i := 0; i < len(tc.nodes); i++ {
				toAdd := tc.nodes[len(tc.nodes)-1-i]

				// We intentionally ignore the errors here to get to the same member state
				err := reverseRing.Add(toAdd)
				if !errors.Is(err, ErrMemberAlreadyExists) {
					require.Nil(t, err)
				}
			}

			// Check that the findValues match for a few keys in both the reverse built and normal
			if len(successfulNodes) > 0 {
				for i := 0; i < 100; i++ {
					key := []byte(strconv.Itoa(i))
					found, err := ring.FindN(key, 1)
					require.NoError(t, err)

					reverseFound, err := reverseRing.FindN(key, 1)
					require.NoError(t, err)

					require.Equal(t, found[0].Key(), reverseFound[0].Key())
				}
			}

			// Empty out the nodes
			for _, testNodeInfo := range tc.nodes {
				err := ring.Remove(testNodeInfo)
				if testNodeInfo.addNodeError == nil {
					require.NoError(t, err)
					delete(successfulNodes, testNodeInfo.nodeKeyAndValue)
				} else {
					require.Equal(t, ErrMemberNotFound, err)
				}

				require.Len(t, ring.virtualNodes, len(successfulNodes)*int(tc.replicationFactor))
				require.Len(t, ring.nodes, len(successfulNodes))
			}
		})
	}
}

const numTestKeys = 1_000_000

func TestBackendBalance(t *testing.T) {
	hasherFunc := xxhash.Sum64

	testCases := []int{1, 2, 3, 5, 10, 100}

	for _, numMembers := range testCases {
		numMembers := numMembers
		t.Run(strconv.Itoa(numMembers), func(t *testing.T) {
			t.Parallel()

			ring, err := New(hasherFunc, 100)
			require.NoError(t, err)

			memberKeyCount := map[member]int{}

			for memberNum := 0; memberNum < numMembers; memberNum++ {
				oneMember := member(memberNum)
				err := ring.Add(oneMember)
				require.Nil(t, err)
				memberKeyCount[oneMember] = 0
			}

			require.Len(t, ring.Members(), numMembers)

			for i := 0; i < numTestKeys; i++ {
				found, err := ring.FindN([]byte(strconv.Itoa(i)), 1)
				require.NoError(t, err)
				require.Len(t, found, 1)

				memberKeyCount[found[0].(member)]++
			}

			totalKeysDistributed := 0
			mean := float64(numTestKeys) / float64(numMembers)
			stddevSum := 0.0
			for _, memberKeyCount := range memberKeyCount {
				totalKeysDistributed += memberKeyCount
				stddevSum += math.Pow(float64(memberKeyCount)-mean, 2)
			}
			require.Equal(t, numTestKeys, totalKeysDistributed)

			stddev := math.Sqrt(stddevSum / float64(numMembers))

			// We want the stddev to be less than 10% of the mean with 100 virtual nodes
			require.Less(t, stddev, mean*.1)
		})
	}
}

type perturbationKind int

const (
	add perturbationKind = iota
	remove
)

// perturb randomly adds or removes a node from the ring
// it returns the mapping from before the ring was changed, the way the ring was
// modified (add/remove/identity), and the member that was affected
// (added, removed, or none)
func perturb(tb testing.TB, ring *Ring, spread uint8,
	numTestKeys int) (before map[string][]Member,
	perturbation perturbationKind, affectedMember member,
) {
	before = make(map[string][]Member)
	for i := 0; i < numTestKeys; i++ {
		found, err := ring.FindN([]byte(strconv.Itoa(i)), spread)
		require.NoError(tb, err)
		require.Len(tb, found, int(spread))
		before[strconv.Itoa(i)] = found
	}

	// pick a random perturbation - add or remove a single node
	perturbation = perturbationKind(rand.Intn(2))

	// don't let the ring dip below the spread
	if len(ring.Members()) == int(spread) {
		perturbation = add
	}

	switch perturbation {
	case add:
		err := errors.New("intentionally blank")
		for err != nil {
			affectedMember = member(rand.Int())
			err = ring.Add(affectedMember)
		}
	case remove:
		i := rand.Intn(len(ring.Members()))
		affectedMember = ring.Members()[i].(member)
		require.NoError(tb, ring.Remove(affectedMember))
	}
	return
}

// verify takes a ring, a change that has already been applied to the ring
// (add/remove node) and the state of the ring before the change happened, and
// asserts that the keys were remapped correctly.
func verify(tb testing.TB, ring *Ring,
	before map[string][]Member, perturbation perturbationKind,
	affectedMember member, spread uint8, numTestKeys int,
) {
	for i := 0; i < numTestKeys; i++ {
		key := strconv.Itoa(i)
		found, err := ring.FindN([]byte(key), spread)
		require.NoError(tb, err)
		require.Len(tb, found, int(spread))

		switch perturbation {
		case remove:
			// any key that didn't map to the removed node remains the same
			for _, n := range before[key] {
				if n.Key() == affectedMember.Key() {
					continue
				}
				require.Contains(tb, found, n)
			}
		case add:
			// at most one key should be different,
			// and it should only differ by the new key
			foundMinusAffected := make([]Member, 0)
			affectedCount := 0
			for _, n := range found {
				if n == affectedMember {
					affectedCount++
					continue
				}
				foundMinusAffected = append(foundMinusAffected, n)
			}
			require.LessOrEqual(tb, affectedCount, 1)
			require.Subset(tb, before[key], foundMinusAffected, "before: %#v\nafter: %#v", before[key], found)
			if len(foundMinusAffected) == len(found) {
				require.EqualValues(tb, found, before[key])
			}
		default:
			require.Fail(tb, "invalid perturbation")
		}
	}
}

func TestConsistency(t *testing.T) {
	ring, err := New(xxhash.Sum64, 100)
	require.NoError(t, err)

	for memberNum := 0; memberNum < 5; memberNum++ {
		require.NoError(t, ring.Add(member(memberNum)))
	}

	spread := uint8(3)
	numTestKeys := 1000
	for i := 0; i < 10; i++ {
		before, perturbation, affectedMember := perturb(t, ring, spread, numTestKeys)
		verify(t, ring, before, perturbation, affectedMember, spread, numTestKeys)
	}
}

func BenchmarkRemapping(b *testing.B) {
	numKeys := 1000
	numMembers := 5

	ring, err := New(xxhash.Sum64, 100)
	require.NoError(b, err)

	for memberNum := 0; memberNum < numMembers; memberNum++ {
		require.NoError(b, ring.Add(member(memberNum)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		perturb(b, ring, 3, numKeys)
		b.StopTimer()
	}
}

type member int

func (m member) Key() string {
	return fmt.Sprintf("member-%d", m)
}

// tableHash is a HashFunc backed by a lookup table.
// It lets a test place members, virtual nodes and keys at exact positions on
// the ring, which a real hash function makes practically impossible.
// Hashing an input that is missing from the table fails the test.
type tableHash struct {
	t      *testing.T
	hashes map[string]uint64
}

func (th tableHash) hash(b []byte) uint64 {
	v, ok := th.hashes[string(b)]
	if !ok {
		th.t.Fatalf("no hash configured for input %q", b)
	}
	return v
}

// vnodeInput mirrors the buffer Add hashes to place a virtual node:
// the member hash followed by the virtual node offset, both little-endian.
func vnodeInput(memberHash uint64, offset uint16) string {
	buf := make([]byte, 10)
	binary.LittleEndian.PutUint64(buf, memberHash)
	binary.LittleEndian.PutUint16(buf[8:], offset)
	return string(buf)
}

func memberKeys(members []Member) []string {
	keys := make([]string, 0, len(members))
	for _, m := range members {
		keys = append(keys, m.Key())
	}
	return keys
}

// A key owned by the first virtual node at or after its hash.
// A key hashing exactly onto a virtual node belongs to that node, and a key
// hashing past the last virtual node wraps around to the first one.
func TestFindNWalksTheRingClockwiseFromTheKey(t *testing.T) {
	th := tableHash{t, map[string]uint64{
		"a": 10, vnodeInput(10, 0): 100,
		"b": 20, vnodeInput(20, 0): 200,

		"on-a":     100,
		"between":  150,
		"on-b":     200,
		"past-end": 250,
	}}
	ring := MustNew(th.hash, 1)
	require.NoError(t, ring.Add(testNode{nodeKeyAndValue: "a"}))
	require.NoError(t, ring.Add(testNode{nodeKeyAndValue: "b"}))

	testCases := []struct {
		key  string
		want []string
	}{
		{"on-a", []string{"a", "b"}},
		{"between", []string{"b", "a"}},
		{"on-b", []string{"b", "a"}},
		{"past-end", []string{"a", "b"}},
	}
	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			one, err := ring.FindN([]byte(tc.key), 1)
			require.NoError(t, err)
			require.Equal(t, tc.want[:1], memberKeys(one))

			two, err := ring.FindN([]byte(tc.key), 2)
			require.NoError(t, err)
			require.Equal(t, tc.want, memberKeys(two))
		})
	}
}

// Two members whose keys hash to the same value produce identical virtual
// node hashes.
// They must still be told apart, so that removing one of them never removes
// the other's virtual nodes and the ring shape does not depend on the order
// in which they were added.
func TestMembersWithCollidingHashesStayDistinct(t *testing.T) {
	const rf = 2
	th := tableHash{t, map[string]uint64{
		"a":               10,
		"b":               10,
		vnodeInput(10, 0): 100,
		vnodeInput(10, 1): 300,
		"k":               50,
	}}
	a, b := testNode{nodeKeyAndValue: "a"}, testNode{nodeKeyAndValue: "b"}

	for name, order := range map[string][]testNode{
		"a-then-b": {a, b},
		"b-then-a": {b, a},
	} {
		t.Run(name, func(t *testing.T) {
			ring := MustNew(th.hash, rf)
			for _, m := range order {
				require.NoError(t, ring.Add(m))
			}
			require.Len(t, ring.virtualNodes, 2*rf)

			both, err := ring.FindN([]byte("k"), 2)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"a", "b"}, memberKeys(both))

			// Insertion order must not change which member owns the key.
			first, err := ring.FindN([]byte("k"), 1)
			require.NoError(t, err)
			require.Equal(t, []string{"a"}, memberKeys(first))

			require.NoError(t, ring.Remove(a))
			require.Len(t, ring.virtualNodes, rf)
			require.Equal(t, []string{"b"}, memberKeys(ring.Members()))

			left, err := ring.FindN([]byte("k"), 1)
			require.NoError(t, err)
			require.Equal(t, []string{"b"}, memberKeys(left))

			require.ErrorIs(t, ring.Remove(a), ErrMemberNotFound)
		})
	}
}

func TestNewRejectsAReplicationFactorOfZero(t *testing.T) {
	ring, err := New(xxhash.Sum64, 0)
	require.ErrorIs(t, err, ErrInvalidReplicationFactor)
	require.Nil(t, ring)

	ring, err = New(xxhash.Sum64, 1)
	require.NoError(t, err)
	require.NotNil(t, ring)
}

func TestMustNewPanicsOnlyOnAnInvalidReplicationFactor(t *testing.T) {
	require.PanicsWithError(t, ErrInvalidReplicationFactor.Error(), func() {
		MustNew(xxhash.Sum64, 0)
	})

	var ring *Ring
	require.NotPanics(t, func() { ring = MustNew(xxhash.Sum64, 1) })
	require.NotNil(t, ring)
	require.Equal(t, uint16(1), ring.replicationFactor)
}

// The ring is sorted and searched with cmpVnode, so it must be a strict total
// order with the same sign convention as cmp.Compare.
func TestCmpVnodeOrdersByHashThenMemberHashThenKey(t *testing.T) {
	vn := func(hash, memberHash uint64, key string) virtualNode {
		return virtualNode{hash, nodeRecord{hashvalue: memberHash, nodeKey: key}}
	}

	testCases := []struct {
		name string
		a, b virtualNode
		want int
	}{
		{"lower vnode hash", vn(1, 9, "z"), vn(2, 1, "a"), -1},
		{"higher vnode hash", vn(2, 1, "a"), vn(1, 9, "z"), +1},
		{"same vnode hash, lower member hash", vn(5, 1, "z"), vn(5, 2, "a"), -1},
		{"same vnode hash, higher member hash", vn(5, 2, "a"), vn(5, 1, "z"), +1},
		{"same hashes, lower key", vn(5, 1, "a"), vn(5, 1, "b"), -1},
		{"same hashes, higher key", vn(5, 1, "b"), vn(5, 1, "a"), +1},
		{"identical", vn(5, 1, "a"), vn(5, 1, "a"), 0},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, cmpVnode(tc.a, tc.b))
			require.Equal(t, -tc.want, cmpVnode(tc.b, tc.a))
		})
	}
}
