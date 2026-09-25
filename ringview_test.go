package consistent

import (
	"net/url"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

func targetNamed(t *testing.T, s string) resolver.Target {
	t.Helper()
	u, err := url.Parse(s)
	require.NoError(t, err)
	return resolver.Target{URL: *u}
}

// readyBalancerForTarget builds a balancer from bld for the given target and
// moves every SubConn for addrs to READY.
func readyBalancerForTarget(t *testing.T, bld Builder, target string, addrs ...resolver.Address) *ringBalancer {
	t.Helper()
	cc := newFakeClientConn()
	cc.stateCh = make(chan balancer.State, 64)
	b := bld.Build(cc, balancer.BuildOptions{Target: targetNamed(t, target)}).(*ringBalancer)
	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: addrs},
		BalancerConfig: &BalancerConfig{ReplicationFactor: 100, Spread: 1},
	}))
	for _, sci := range b.subConns.Values() {
		sc := sci.(balancer.SubConn)
		b.UpdateSubConnState(sc, balancer.SubConnState{ConnectivityState: connectivity.Connecting})
		b.UpdateSubConnState(sc, balancer.SubConnState{ConnectivityState: connectivity.Ready})
	}
	return b
}

// RingFor returns a usable view before any balancer exists for the target
// (gRPC builds balancers lazily). The view reports ErrNoRing until a
// balancer publishes a ring.
func TestRingForBeforeBalancerExists(t *testing.T) {
	bld := NewBuilder(xxhash.Sum64)

	view := bld.RingFor("test:///backends")
	require.NotNil(t, view)

	_, err := view.FindN([]byte("key"), 1)
	require.ErrorIs(t, err, ErrNoRing)
	require.Empty(t, view.Members())
}

// A view taken before the balancer exists starts to work once the balancer
// publishes its ring. The view agrees with the ring the picker uses.
func TestRingForSeesTheLiveRing(t *testing.T) {
	bld := NewBuilder(xxhash.Sum64)
	view := bld.RingFor("test:///backends")

	b := readyBalancerForTarget(t, bld, "test:///backends",
		resolver.Address{ServerName: "t", Addr: "1"},
		resolver.Address{ServerName: "t", Addr: "2"},
	)

	require.ElementsMatch(t, []string{"t1", "t2"}, keys(view.Members()))

	for _, key := range []string{"a", "b", "c", "d"} {
		got, err := view.FindN([]byte(key), 1)
		require.NoError(t, err)
		want, err := b.hashring.FindN([]byte(key), 1)
		require.NoError(t, err)
		require.Equal(t, want[0].Key(), got[0].Key(),
			"view and picker must agree on the owner of %q", key)
	}
}

// A view that the caller already holds shows membership changes.
func TestRingForReflectsMembershipChanges(t *testing.T) {
	bld := NewBuilder(xxhash.Sum64)
	view := bld.RingFor("test:///backends")

	addrs := []resolver.Address{{ServerName: "t", Addr: "1"}, {ServerName: "t", Addr: "2"}}
	b := readyBalancerForTarget(t, bld, "test:///backends", addrs...)
	require.ElementsMatch(t, []string{"t1", "t2"}, keys(view.Members()))

	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: resolver.State{Addresses: addrs[:1]},
	}))
	require.ElementsMatch(t, []string{"t1"}, keys(view.Members()),
		"a removed backend must disappear from held views")
}

// A ReplicationFactor change replaces the ring object. Held views must
// follow the replacement rather than serve the stale ring.
func TestRingForFollowsRingReplacement(t *testing.T) {
	bld := NewBuilder(xxhash.Sum64)
	view := bld.RingFor("test:///backends")

	addrs := []resolver.Address{{ServerName: "t", Addr: "1"}, {ServerName: "t", Addr: "2"}}
	b := readyBalancerForTarget(t, bld, "test:///backends", addrs...)
	oldRing := b.hashring

	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: addrs},
		BalancerConfig: &BalancerConfig{ReplicationFactor: 7, Spread: 1},
	}))
	require.NotSame(t, oldRing, b.hashring, "sanity: the ring was replaced")

	require.ElementsMatch(t, []string{"t1", "t2"}, keys(view.Members()))
	got, err := view.FindN([]byte("key"), 1)
	require.NoError(t, err)
	want, err := b.hashring.FindN([]byte("key"), 1)
	require.NoError(t, err)
	require.Equal(t, want[0].Key(), got[0].Key(), "view must consult the replacement ring")
}

// Closing the balancer empties its views. A view must not continue to serve
// a ring that no longer routes anything.
func TestRingForEmptyAfterClose(t *testing.T) {
	bld := NewBuilder(xxhash.Sum64)
	view := bld.RingFor("test:///backends")

	b := readyBalancerForTarget(t, bld, "test:///backends",
		resolver.Address{ServerName: "t", Addr: "1"})
	require.NotEmpty(t, view.Members())

	b.Close()
	_, err := view.FindN([]byte("key"), 1)
	require.ErrorIs(t, err, ErrNoRing)
	require.Empty(t, view.Members())
}

// Views are per-target: two targets that the same builder serves get
// separate views.
func TestRingForIsPerTarget(t *testing.T) {
	bld := NewBuilder(xxhash.Sum64)

	readyBalancerForTarget(t, bld, "test:///alpha", resolver.Address{ServerName: "a", Addr: "1"})
	readyBalancerForTarget(t, bld, "test:///beta", resolver.Address{ServerName: "b", Addr: "1"})

	require.ElementsMatch(t, []string{"a1"}, keys(bld.RingFor("test:///alpha").Members()))
	require.ElementsMatch(t, []string{"b1"}, keys(bld.RingFor("test:///beta").Members()))
}
