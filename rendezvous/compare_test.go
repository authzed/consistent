package rendezvous

import (
	"fmt"
	"math"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"

	"github.com/authzed/consistent/hashring"
)

// The tests and benchmarks in this file compare rendezvous hashing against the
// vnode hashring. Run the distribution report with:
//
//	go test ./rendezvous -run TestCompareDistribution -v
//
// and the benchmarks with:
//
//	go test ./rendezvous -run '^$' -bench . -benchmem

var implementations = []struct {
	name string
	new  func() ringLike
}{
	{"ring-r100", func() ringLike { return hashring.MustNew(xxhash.Sum64, 100) }},
	{"ring-r1000", func() ringLike { return hashring.MustNew(xxhash.Sum64, 1000) }},
	{"hrw", func() ringLike { return New(xxhash.Sum64) }},
}

// TestCompareDistribution reports, for each implementation and member count:
//
//   - max/mean: the most loaded member's share relative to a perfect 1/n
//   - cov: coefficient of variation of per-member load
//   - remove moved: fraction of keys that change owner when a member leaves
//     (ideal is 1/n)
//   - max absorb: the largest share of the departed member's keys taken by a
//     single survivor, relative to a perfect 1/(n-1) split
func TestCompareDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("distribution report is slow")
	}

	const numKeys = 200_000
	ks := keys(numKeys, 42)

	t.Logf("%-11s %5s %9s %7s %13s %11s", "impl", "n", "max/mean", "cov", "remove moved", "max absorb")
	for _, n := range []int{3, 5, 10, 30, 100} {
		for _, impl := range implementations {
			s := impl.new()
			ns := nodes(n)
			for _, node := range ns {
				require.NoError(t, s.Add(node))
			}

			before := owners(t, s, ks)
			load := map[string]int{}
			for _, o := range before {
				load[o]++
			}
			maxLoad, sumSq := 0, 0.0
			mean := float64(numKeys) / float64(n)
			for _, node := range ns {
				l := load[node.Key()]
				maxLoad = max(maxLoad, l)
				sumSq += (float64(l) - mean) * (float64(l) - mean)
			}
			cov := math.Sqrt(sumSq/float64(n)) / mean

			// Remove the most loaded member, since that is the worst case for
			// how its keys are redistributed.
			var departed testNode
			for _, node := range ns {
				if load[node.Key()] == maxLoad {
					departed = node
					break
				}
			}
			require.NoError(t, s.Remove(departed))
			after := owners(t, s, ks)

			moved := 0
			absorbed := map[string]int{}
			for i := range ks {
				if before[i] != after[i] {
					moved++
					absorbed[after[i]]++
				}
			}
			maxAbsorbed := 0
			for _, a := range absorbed {
				maxAbsorbed = max(maxAbsorbed, a)
			}
			fairAbsorb := float64(moved) / float64(n-1)

			t.Logf("%-11s %5d %9.3f %7.3f %13.3f %11.3f",
				impl.name, n, float64(maxLoad)/mean, cov,
				float64(moved)/numKeys, float64(maxAbsorbed)/fairAbsorb)
		}
	}
}

func benchSizes() []int { return []int{3, 10, 30, 100, 1000} }

// BenchmarkFindN measures a single lookup, which is what the balancer's
// picker does for every dispatched RPC.
func BenchmarkFindN(b *testing.B) {
	// Cycle through many keys so the branch predictor cannot memorize the
	// path through the lookup.
	ks := keys(4096, 7)

	for _, spread := range []uint8{1, 2} {
		for _, n := range benchSizes() {
			for _, impl := range implementations {
				b.Run(fmt.Sprintf("spread=%d/n=%d/%s", spread, n, impl.name), func(b *testing.B) {
					s := impl.new()
					for _, node := range nodes(n) {
						require.NoError(b, s.Add(node))
					}

					b.ReportAllocs()
					i := 0
					for b.Loop() {
						if _, err := s.FindN(ks[i&(len(ks)-1)], spread); err != nil {
							b.Fatal(err)
						}
						i++
					}
				})
			}
		}
	}
}

// BenchmarkFindNParallel measures lookups from many goroutines at once, since
// every in-flight dispatch picks concurrently against the same structure.
func BenchmarkFindNParallel(b *testing.B) {
	ks := keys(4096, 8)

	for _, n := range []int{10, 30} {
		for _, impl := range implementations {
			b.Run(fmt.Sprintf("n=%d/%s", n, impl.name), func(b *testing.B) {
				s := impl.new()
				for _, node := range nodes(n) {
					require.NoError(b, s.Add(node))
				}

				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						if _, err := s.FindN(ks[i&(len(ks)-1)], 1); err != nil {
							b.Error(err)
							return
						}
						i++
					}
				})
			})
		}
	}
}

// BenchmarkMembershipChange measures one member leaving and rejoining, which
// happens whenever a backend's connectivity state changes.
func BenchmarkMembershipChange(b *testing.B) {
	// n=1000 is omitted: the ring re-sorts up to a million vnodes per change,
	// which takes minutes to benchmark.
	for _, n := range []int{3, 10, 30, 100} {
		for _, impl := range implementations {
			b.Run(fmt.Sprintf("n=%d/%s", n, impl.name), func(b *testing.B) {
				s := impl.new()
				ns := nodes(n)
				for _, node := range ns {
					require.NoError(b, s.Add(node))
				}

				b.ReportAllocs()
				for b.Loop() {
					if err := s.Remove(ns[0]); err != nil {
						b.Fatal(err)
					}
					if err := s.Add(ns[0]); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
