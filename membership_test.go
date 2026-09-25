package consistent

import (
	"fmt"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

// A SubConn joins the ring when the resolver provides it, before its
// connection is READY. IDLE and CONNECTING backends keep their keyspace.
func TestNewSubConnJoinsRingBeforeReady(t *testing.T) {
	cc := newFakeClientConn()
	go func() {
		for range cc.stateCh {
		}
	}()

	b := NewBuilder(xxhash.Sum64).Build(cc, balancer.BuildOptions{}).(*ringBalancer)
	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: []resolver.Address{{ServerName: "t", Addr: "1"}}},
		BalancerConfig: &BalancerConfig{ReplicationFactor: 100, Spread: 1},
	}))

	require.Equal(t, []string{"t1"}, ringKeys(b),
		"a resolver-provided backend must own its keys before it is READY")
}

// A READY SubConn that moves to IDLE keeps its ring membership and
// reconnects. Servers that limit connection age send GOAWAY on an interval,
// and that produces exactly this transition on a healthy backend.
func TestIdleSubConnKeepsRingMembership(t *testing.T) {
	addr := resolver.Address{ServerName: "t", Addr: "1"}
	b, _ := readyBalancer(t, addr)
	sci, _ := b.subConns.Get(addr)
	sc := sci.(*fakeSubConn)
	connectsSoFar := sc.connectCalls

	b.UpdateSubConnState(sc, balancer.SubConnState{ConnectivityState: connectivity.Idle})

	require.Equal(t, []string{"t1"}, ringKeys(b),
		"a healthy backend must not lose its keys on a connection recycle")
	require.Equal(t, connectsSoFar+1, sc.connectCalls, "IDLE must trigger a reconnect")
}

// A ReplicationFactor change rebuilds the ring. The new ring must contain
// every member except those in TRANSIENT_FAILURE.
func TestReplicationFactorChangeKeepsNonFailedMembers(t *testing.T) {
	addrs := []resolver.Address{
		{ServerName: "t", Addr: "1"},
		{ServerName: "t", Addr: "2"},
		{ServerName: "t", Addr: "3"},
	}
	b, _ := readyBalancer(t, addrs...)

	// t2 fails, t3 reconnects: only the failure may cost ring membership.
	sci2, _ := b.subConns.Get(addrs[1])
	b.UpdateSubConnState(sci2.(balancer.SubConn), balancer.SubConnState{
		ConnectivityState: connectivity.TransientFailure,
		ConnectionError:   fmt.Errorf("refused"),
	})
	sci3, _ := b.subConns.Get(addrs[2])
	b.UpdateSubConnState(sci3.(balancer.SubConn), balancer.SubConnState{
		ConnectivityState: connectivity.Connecting,
	})
	require.ElementsMatch(t, []string{"t1", "t3"}, ringKeys(b))

	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: addrs},
		BalancerConfig: &BalancerConfig{ReplicationFactor: 7, Spread: 1},
	}))
	require.ElementsMatch(t, []string{"t1", "t3"}, ringKeys(b),
		"the rebuilt ring must keep the reconnecting member and exclude the failed one")
}
