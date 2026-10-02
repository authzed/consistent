package e2e

import (
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Servers that limit connection age send GOAWAY on an interval, and the
// client reconnects each time. Reconnecting must not move keys: callers that
// cache by ring owner lose locality on every remap.
func TestGoawayChurnKeepsKeyPlacement(t *testing.T) {
	const maxAge = time.Second
	e := newEnv(t, 3, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionAge:      maxAge,
		MaxConnectionAgeGrace: 5 * time.Second,
	}))
	r := e.startLoad(e.dial())
	defer e.writeResults(r)
	e.waitSteady(r)

	acceptsBefore := make(map[string]int64, len(e.backends))
	for _, b := range e.backends {
		acceptsBefore[b.id] = b.lis.accepts.Load()
	}
	from := r.mark("churn")
	time.Sleep(8 * time.Second)
	to := r.mark("end")
	r.stop()

	samples := r.snapshot()
	e.summarize("goaway churn", window(samples, e.keys, from, to))
	for _, b := range e.backends {
		reconnects := b.lis.accepts.Load() - acceptsBefore[b.id]
		t.Logf("%s: %d reconnects", b.id, reconnects)
		require.GreaterOrEqual(t, reconnects, int64(4), "%s was not recycled; the test did not exercise GOAWAY", b.id)
	}
	e.requireUndisturbed(samples, e.keys, from, to)
}

// A backend that goes away closes its connections and refuses new ones. Its
// keys move to the next closest backend right away, and move back once it
// returns. No other key moves.
func TestNodeDeathFailsOverAndRecovers(t *testing.T) {
	e := newEnv(t, 3)
	r := e.startLoad(e.dial())
	defer e.writeResults(r)
	e.waitSteady(r)

	victim := e.backends[0].id
	victimKeys, otherKeys := e.partitionKeys(victim)

	down := r.mark("disable " + victim)
	require.NoError(t, e.proxies[victim].Disable())
	time.Sleep(4 * time.Second)
	up := r.mark("enable " + victim)
	require.NoError(t, e.proxies[victim].Enable())
	time.Sleep(5 * time.Second)
	end := r.mark("end")
	r.stop()

	samples := r.snapshot()
	e.summarize("victim keys, down", window(samples, victimKeys, down, up))
	e.summarize("victim keys, recovered", window(samples, victimKeys, up, end))
	e.summarize("other keys", window(samples, otherKeys, down, end))

	failover := e.settleTime(samples, victimKeys, down, up, func(k []byte) string { return e.owner(k, victim) })
	recovery := e.settleTime(samples, victimKeys, up, end, func(k []byte) string { return victim })
	t.Logf("failover took %v, recovery took %v", failover, recovery)

	require.Less(t, failover, time.Second, "keys of a dead backend must move promptly")
	require.Less(t, recovery, maxBackoff+2*time.Second, "keys must return once the backend is back")
	e.requireUndisturbed(samples, otherKeys, down, end)
}

// A network partition leaves an established connection open but silent. The
// client only notices through keepalive. The reconnect then stalls on the
// handshake until the dial timeout. After both, the backend leaves the ring
// and its keys move. This is the case the balancer cannot see without
// keepalive: the SubConn stays READY until the transport is closed.
func TestSilentConnectionFailsOverAfterKeepalive(t *testing.T) {
	e := newEnv(t, 3)
	r := e.startLoad(e.dial())
	defer e.writeResults(r)
	e.waitSteady(r)

	victim := e.backends[0].id
	victimKeys, otherKeys := e.partitionKeys(victim)
	proxy := e.proxies[victim]

	// A timeout toxic with timeout 0 holds every byte in that direction
	// until the toxic is removed, without closing the connection. It also
	// applies to new connections, so a reconnect accepts TCP and then hangs.
	cut := r.mark("partition " + victim)
	for _, stream := range []string{"upstream", "downstream"} {
		_, err := proxy.AddToxic("silence_"+stream, "timeout", stream, 1, toxiproxy.Attributes{"timeout": 0})
		require.NoError(t, err)
	}
	expectedDetection := keepaliveTime + keepaliveTimeout + minConnectTimeout
	time.Sleep(expectedDetection + 4*time.Second)
	heal := r.mark("heal " + victim)
	for _, stream := range []string{"upstream", "downstream"} {
		require.NoError(t, proxy.RemoveToxic("silence_"+stream))
	}
	time.Sleep(5 * time.Second)
	end := r.mark("end")
	r.stop()

	samples := r.snapshot()
	e.summarize("victim keys, partitioned", window(samples, victimKeys, cut, heal))
	e.summarize("victim keys, healed", window(samples, victimKeys, heal, end))
	e.summarize("other keys", window(samples, otherKeys, cut, end))

	failover := e.settleTime(samples, victimKeys, cut, heal, func(k []byte) string { return e.owner(k, victim) })
	recovery := e.settleTime(samples, victimKeys, heal, end, func(k []byte) string { return victim })
	t.Logf("failover took %v (keepalive %v + timeout %v + dial %v = %v), recovery took %v",
		failover, keepaliveTime, keepaliveTimeout, minConnectTimeout, expectedDetection, recovery)

	require.Less(t, failover, expectedDetection+time.Second, "keys must move once keepalive and the dial timeout expire")
	require.Less(t, recovery, maxBackoff+2*time.Second, "keys must return once the partition heals")
	e.requireUndisturbed(samples, otherKeys, cut, end)
}

// partitionKeys splits the test keys into those that id owns on the full
// ring and the rest.
func (e *env) partitionKeys(id string) (owned, others [][]byte) {
	for _, k := range e.keys {
		if e.home[string(k)] == id {
			owned = append(owned, k)
		} else {
			others = append(others, k)
		}
	}
	return owned, others
}
