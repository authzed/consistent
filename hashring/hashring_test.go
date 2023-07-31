package hashring

import (
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
