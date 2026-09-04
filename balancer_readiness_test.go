package consistent

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	"github.com/authzed/consistent/hashring"
)

// keyHashingTo returns a key that the ring places on the member with the
// given key, so a test can target a specific backend.
func keyHashingTo(t *testing.T, ring *hashring.Ring, memberKey string) []byte {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		members, err := ring.FindN(key, 1)
		require.NoError(t, err)
		if members[0].Key() == memberKey {
			return key
		}
	}
	t.Fatalf("no key hashes to %s", memberKey)
	return nil
}

// TestPickerSkipsSubConnsThatAreNotReady drives the balancer directly: two
// backends come up, then one fails. Keys that hashed to the failed backend
// must be picked on the remaining ready one instead of a dead SubConn.
func TestPickerSkipsSubConnsThatAreNotReady(t *testing.T) {
	cc := newFakeClientConn()
	// Drain state updates so the balancer never blocks on the fake conn.
	go func() {
		for range cc.stateCh {
		}
	}()

	b := NewBuilder(xxhash.Sum64).Build(cc, balancer.BuildOptions{}).(*ringBalancer)
	addrs := []resolver.Address{{ServerName: "t", Addr: "1"}, {ServerName: "t", Addr: "2"}}
	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: addrs},
		BalancerConfig: &BalancerConfig{ReplicationFactor: 100, Spread: 1},
	}))

	subConnFor := func(addr resolver.Address) balancer.SubConn {
		sc, ok := b.subConns.Get(addr)
		require.True(t, ok)
		return sc.(balancer.SubConn)
	}
	sc1, sc2 := subConnFor(addrs[0]), subConnFor(addrs[1])
	for _, sc := range []balancer.SubConn{sc1, sc2} {
		b.UpdateSubConnState(sc, balancer.SubConnState{ConnectivityState: connectivity.Connecting})
		b.UpdateSubConnState(sc, balancer.SubConnState{ConnectivityState: connectivity.Ready})
	}

	// Build a reference ring to learn which keys land on backend 2.
	ref := hashring.MustNew(xxhash.Sum64, 100)
	for _, a := range addrs {
		require.NoError(t, ref.Add(subConnMember{key: a.ServerName + a.Addr}))
	}
	keyFor2 := keyHashingTo(t, ref, "t2")

	pick := func(key []byte) balancer.SubConn {
		res, err := b.picker.Pick(balancer.PickInfo{Ctx: context.WithValue(context.Background(), CtxKey, key)})
		require.NoError(t, err)
		return res.SubConn
	}
	require.Same(t, sc2, pick(keyFor2), "sanity: key routes to backend 2 while it is ready")

	// Backend 2 dies.
	b.UpdateSubConnState(sc2, balancer.SubConnState{
		ConnectivityState: connectivity.TransientFailure,
		ConnectionError:   fmt.Errorf("connection refused"),
	})
	require.Same(t, sc1, pick(keyFor2), "key must move to the only ready backend")

	// Backend 2 comes back and is used again.
	b.UpdateSubConnState(sc2, balancer.SubConnState{ConnectivityState: connectivity.Idle})
	b.UpdateSubConnState(sc2, balancer.SubConnState{ConnectivityState: connectivity.Connecting})
	b.UpdateSubConnState(sc2, balancer.SubConnState{ConnectivityState: connectivity.Ready})
	require.Same(t, sc2, pick(keyFor2), "key returns to backend 2 once it is ready")
}

// TestRPCsAreNotStuckBehindAConnectingBackend reproduces a peer whose address
// is still resolvable but never completes a connection (a killed pod whose IP
// is still in the endpoint list). RPCs hashed to it must be served by the
// healthy backend instead of waiting for the dial to time out.
func TestRPCsAreNotStuckBehindAConnectingBackend(t *testing.T) {
	// Healthy backend: a real gRPC server with the health service.
	healthyLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(healthyLis) }()
	t.Cleanup(srv.Stop)

	// Black hole: accepts TCP connections but never speaks HTTP/2, so the
	// SubConn stays CONNECTING until gRPC's 20s connect timeout.
	blackholeLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = blackholeLis.Close() })
	go func() {
		for {
			conn, err := blackholeLis.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()

	balancer.Register(NewBuilder(xxhash.Sum64))
	addrs := []resolver.Address{{Addr: healthyLis.Addr().String()}, {Addr: blackholeLis.Addr().String()}}
	rb := manual.NewBuilderWithScheme("readiness")
	rb.InitialState(resolver.State{Addresses: addrs})

	svcConfig, err := (&BalancerConfig{ReplicationFactor: 100, Spread: 1}).ServiceConfigJSON()
	require.NoError(t, err)
	conn, err := grpc.NewClient(rb.Scheme()+":///backends",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(rb),
		grpc.WithDefaultServiceConfig(svcConfig),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := healthpb.NewHealthClient(conn)

	ref := hashring.MustNew(xxhash.Sum64, 100)
	for _, a := range addrs {
		require.NoError(t, ref.Add(subConnMember{key: a.ServerName + a.Addr}))
	}
	keyForHealthy := keyHashingTo(t, ref, addrs[0].Addr)
	keyForBlackhole := keyHashingTo(t, ref, addrs[1].Addr)

	check := func(key []byte, timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		ctx = context.WithValue(ctx, CtxKey, key)
		_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		return err
	}

	// Warm up: the healthy backend is reachable and connected.
	require.NoError(t, check(keyForHealthy, 5*time.Second))

	// A key hashed to the black-holed backend must still be answered promptly.
	err = check(keyForBlackhole, 3*time.Second)
	if st, ok := status.FromError(err); ok && st.Code() == codes.DeadlineExceeded {
		t.Fatalf("RPC hung behind a CONNECTING backend instead of being served by the ready one: %v", err)
	}
	require.NoError(t, err)
}
