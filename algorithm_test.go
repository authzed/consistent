package consistent

import (
	"context"
	"fmt"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"

	"github.com/authzed/consistent/hashring"
	"github.com/authzed/consistent/rendezvous"
)

func TestParseConfigAlgorithm(t *testing.T) {
	bld := NewBuilder(xxhash.Sum64)

	cfg, err := bld.ParseConfig([]byte(`{"algorithm": "rendezvous"}`))
	require.NoError(t, err)
	require.Equal(t, &BalancerConfig{
		ReplicationFactor: DefaultReplicationFactor,
		Spread:            DefaultSpread,
		Algorithm:         AlgorithmRendezvous,
	}, cfg)

	cfg, err = bld.ParseConfig([]byte(`{"algorithm": "hashring"}`))
	require.NoError(t, err)
	require.Equal(t, AlgorithmHashring, cfg.(*BalancerConfig).Algorithm)

	_, err = bld.ParseConfig([]byte(`{"algorithm": "maglev"}`))
	require.ErrorContains(t, err, `unknown algorithm "maglev"`)
}

func TestServiceConfigJSONAlgorithm(t *testing.T) {
	got, err := (&BalancerConfig{Spread: 1, Algorithm: AlgorithmRendezvous}).ServiceConfigJSON()
	require.NoError(t, err)
	require.Equal(t, `{"loadBalancingConfig":[{"consistent-hashring":{"spread":1,"algorithm":"rendezvous"}}]}`, got)
}

// rendezvousBalancer builds a balancer that uses rendezvous hashing and
// moves every SubConn for addrs to READY.
func rendezvousBalancer(t *testing.T, addrs ...resolver.Address) *ringBalancer {
	t.Helper()
	b, _ := readyBalancer(t, addrs...)
	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: addrs},
		BalancerConfig: &BalancerConfig{ReplicationFactor: 100, Spread: 1, Algorithm: AlgorithmRendezvous},
	}))
	require.IsType(t, &rendezvous.Set{}, b.hashring)
	return b
}

// Switching algorithms rebuilds the member set. The new set must contain
// every member except those in TRANSIENT_FAILURE, and switching back must
// restore a hashring.
func TestAlgorithmChangeRebuildsMemberSet(t *testing.T) {
	addrs := []resolver.Address{
		{ServerName: "t", Addr: "1"},
		{ServerName: "t", Addr: "2"},
		{ServerName: "t", Addr: "3"},
	}
	b, _ := readyBalancer(t, addrs...)
	require.IsType(t, &hashring.Ring{}, b.hashring)

	sci2, _ := b.subConns.Get(addrs[1])
	b.UpdateSubConnState(sci2.(balancer.SubConn), balancer.SubConnState{
		ConnectivityState: connectivity.TransientFailure,
		ConnectionError:   fmt.Errorf("refused"),
	})

	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: addrs},
		BalancerConfig: &BalancerConfig{ReplicationFactor: 100, Spread: 1, Algorithm: AlgorithmRendezvous},
	}))
	require.IsType(t, &rendezvous.Set{}, b.hashring)
	require.ElementsMatch(t, []string{"t1", "t3"}, ringKeys(b))

	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: addrs},
		BalancerConfig: &BalancerConfig{ReplicationFactor: 100, Spread: 1},
	}))
	require.IsType(t, &hashring.Ring{}, b.hashring)
	require.ElementsMatch(t, []string{"t1", "t3"}, ringKeys(b))
}

// Rendezvous hashing has no virtual nodes, so a ReplicationFactor change must
// not rebuild the member set and move keys.
func TestRendezvousIgnoresReplicationFactor(t *testing.T) {
	addrs := []resolver.Address{{ServerName: "t", Addr: "1"}, {ServerName: "t", Addr: "2"}}
	b := rendezvousBalancer(t, addrs...)
	before := b.hashring

	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: addrs},
		BalancerConfig: &BalancerConfig{ReplicationFactor: 7, Spread: 1, Algorithm: AlgorithmRendezvous},
	}))
	require.Same(t, before, b.hashring)
}

// With rendezvous hashing, the picker, the RingView, and a standalone
// rendezvous.Set with the same members all agree, and a failed backend's
// keys move to a remaining one.
func TestRendezvousPickerRoutes(t *testing.T) {
	bld := NewBuilder(xxhash.Sum64)
	addrs := []resolver.Address{{ServerName: "t", Addr: "1"}, {ServerName: "t", Addr: "2"}}
	b := readyBalancerForTarget(t, bld, "test:///backends", addrs...)
	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: addrs},
		BalancerConfig: &BalancerConfig{ReplicationFactor: 100, Spread: 1, Algorithm: AlgorithmRendezvous},
	}))
	view := bld.RingFor("test:///backends")

	ref := rendezvous.New(xxhash.Sum64)
	for _, a := range addrs {
		require.NoError(t, ref.Add(subConnMember{key: a.ServerName + a.Addr}))
	}

	pick := func(key []byte) balancer.SubConn {
		res, err := b.picker.Pick(balancer.PickInfo{Ctx: context.WithValue(context.Background(), CtxKey, key)})
		require.NoError(t, err)
		return res.SubConn
	}

	var keyFor2 []byte
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		want, err := ref.FindN(key, 1)
		require.NoError(t, err)

		viewed, err := view.FindN(key, 1)
		require.NoError(t, err)
		require.Equal(t, want[0].Key(), viewed[0].Key())

		picked := pick(key)
		require.Equal(t, want[0].Key(), b.scKeys[picked])

		if want[0].Key() == "t2" {
			keyFor2 = key
		}
	}
	require.NotNil(t, keyFor2, "no key hashes to t2")

	sci2, _ := b.subConns.Get(addrs[1])
	b.UpdateSubConnState(sci2.(balancer.SubConn), balancer.SubConnState{
		ConnectivityState: connectivity.TransientFailure,
		ConnectionError:   fmt.Errorf("refused"),
	})
	sci1, _ := b.subConns.Get(addrs[0])
	require.Equal(t, sci1.(balancer.SubConn), pick(keyFor2))
}
