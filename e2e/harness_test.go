package e2e

import (
	"context"
	"encoding/csv"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tctoxiproxy "github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	"github.com/authzed/consistent"
	"github.com/authzed/consistent/hashring"
)

const (
	toxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"
	// firstProxiedPort is where the toxiproxy module starts assigning proxy
	// listen ports inside the container, one per WithProxy option.
	firstProxiedPort = 8666

	replicationFactor = 100
	numKeys           = 90
	// requestInterval is the pause between RPCs for a single key. Every key
	// has its own loop, so a stalled key never delays the others.
	requestInterval = 20 * time.Millisecond
	rpcTimeout      = 2 * time.Second
	// stallThreshold is the latency above which an RPC waited on a
	// connection rather than being served by a READY one. Healthy RPCs
	// through the proxy take a few milliseconds.
	stallThreshold = 100 * time.Millisecond

	// Client connection settings. The dial timeout bounds how long keys wait
	// on a backend that accepts TCP but never completes the handshake.
	minConnectTimeout = 500 * time.Millisecond
	maxBackoff        = time.Second
	// grpc-go raises any client keepalive time below 10s to 10s.
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = time.Second

	// backendHeader carries the ID of the backend that served an RPC.
	backendHeader = "x-backend-id"
)

func init() {
	balancer.Register(consistent.NewBuilder(xxhash.Sum64))
}

// countingListener counts accepted connections, which shows how often the
// client reconnected to a backend.
type countingListener struct {
	net.Listener
	accepts atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return conn, err
}

// backend is a gRPC health server in the test process. It reports its ID in
// a response header on every RPC.
type backend struct {
	id  string
	lis *countingListener
}

func (b *backend) port() int { return b.lis.Addr().(*net.TCPAddr).Port }

func startBackend(t *testing.T, id string, opts ...grpc.ServerOption) *backend {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cl := &countingListener{Listener: lis}

	opts = append([]grpc.ServerOption{
		// Allow the client's keepalive pings.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Second,
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			_ = grpc.SetHeader(ctx, metadata.Pairs(backendHeader, id))
			return handler(ctx, req)
		}),
	}, opts...)
	srv := grpc.NewServer(opts...)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(cl) }()
	t.Cleanup(srv.Stop)

	return &backend{id: id, lis: cl}
}

// env is a set of backends, each behind its own toxiproxy proxy, and a client
// that reaches them only through the proxies.
type env struct {
	t        *testing.T
	backends []*backend
	// addrs maps a backend ID to its proxy endpoint. The endpoint is what the
	// resolver hands to the balancer, so it is also the backend's ring key.
	addrs   map[string]string
	proxies map[string]*toxiproxy.Proxy
	keys    [][]byte
	// home maps each key to its owner on the full ring.
	home map[string]string
}

func newEnv(t *testing.T, n int, serverOpts ...grpc.ServerOption) *env {
	t.Helper()
	ctx := t.Context()

	e := &env{
		t:       t,
		addrs:   make(map[string]string, n),
		proxies: make(map[string]*toxiproxy.Proxy, n),
	}

	ports := make([]int, 0, n)
	for i := range n {
		b := startBackend(t, fmt.Sprintf("backend-%d", i), serverOpts...)
		e.backends = append(e.backends, b)
		ports = append(ports, b.port())
	}

	// The proxies reach the in-process backends through testcontainers' host
	// port forwarding.
	opts := []testcontainers.ContainerCustomizer{testcontainers.WithHostPortAccess(ports...)}
	for _, b := range e.backends {
		upstream := net.JoinHostPort(testcontainers.HostInternal, strconv.Itoa(b.port()))
		opts = append(opts, tctoxiproxy.WithProxy(b.id, upstream))
	}
	ctr, err := tctoxiproxy.Run(ctx, toxiproxyImage, opts...)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	uri, err := ctr.URI(ctx)
	require.NoError(t, err)
	client := toxiproxy.NewClient(uri)

	for i, b := range e.backends {
		host, port, err := ctr.ProxiedEndpoint(firstProxiedPort + i)
		require.NoError(t, err)
		e.addrs[b.id] = net.JoinHostPort(host, port)

		proxy, err := client.Proxy(b.id)
		require.NoError(t, err)
		e.proxies[b.id] = proxy
	}

	e.home = make(map[string]string, numKeys)
	for i := range numKeys {
		k := []byte(fmt.Sprintf("key-%d", i))
		e.keys = append(e.keys, k)
		e.home[string(k)] = e.owner(k)
	}
	for _, b := range e.backends {
		owned, _ := e.partitionKeys(b.id)
		require.NotEmpty(t, owned, "no test key hashes to %s", b.id)
	}

	return e
}

func (e *env) dial() healthpb.HealthClient {
	e.t.Helper()
	addrs := make([]resolver.Address, 0, len(e.backends))
	for _, b := range e.backends {
		addrs = append(addrs, resolver.Address{Addr: e.addrs[b.id]})
	}
	rb := manual.NewBuilderWithScheme("e2e")
	rb.InitialState(resolver.State{Addresses: addrs})

	svcConfig := (&consistent.BalancerConfig{ReplicationFactor: replicationFactor, Spread: 1}).MustServiceConfigJSON()
	conn, err := grpc.NewClient(
		rb.Scheme()+":///backends",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(rb),
		grpc.WithDefaultServiceConfig(svcConfig),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: minConnectTimeout,
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   maxBackoff,
			},
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	require.NoError(e.t, err)
	e.t.Cleanup(func() { _ = conn.Close() })
	return healthpb.NewHealthClient(conn)
}

type ringMember string

func (m ringMember) Key() string { return string(m) }

// owner returns the ID of the backend that owns key on a ring of every
// backend except those in excluded.
func (e *env) owner(key []byte, excluded ...string) string {
	ring := hashring.MustNew(xxhash.Sum64, replicationFactor)
	byAddr := make(map[string]string, len(e.backends))
	for _, b := range e.backends {
		if slices.Contains(excluded, b.id) {
			continue
		}
		byAddr[e.addrs[b.id]] = b.id
		require.NoError(e.t, ring.Add(ringMember(e.addrs[b.id])))
	}
	members, err := ring.FindN(key, 1)
	require.NoError(e.t, err)
	return byAddr[members[0].Key()]
}

// sample is one RPC as the client saw it.
type sample struct {
	start   time.Duration // since the recorder started
	latency time.Duration
	key     string
	backend string // empty when the RPC failed
	err     error
}

func (s sample) end() time.Duration { return s.start + s.latency }

type event struct {
	at   time.Duration
	name string
}

// recorder drives one RPC loop per key and records every result.
type recorder struct {
	begin  time.Time
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	samples []sample
	events  []event
	latest  map[string]sample
}

func (e *env) startLoad(client healthpb.HealthClient) *recorder {
	ctx, cancel := context.WithCancel(e.t.Context())
	r := &recorder{begin: time.Now(), cancel: cancel, latest: make(map[string]sample)}
	for _, key := range e.keys {
		r.wg.Go(func() {
			for ctx.Err() == nil {
				r.record(ctx, client, key)
				select {
				case <-ctx.Done():
				case <-time.After(requestInterval):
				}
			}
		})
	}
	e.t.Cleanup(r.stop)
	return r
}

func (r *recorder) record(ctx context.Context, client healthpb.HealthClient, key []byte) {
	rpcCtx, cancel := context.WithTimeout(context.WithValue(ctx, consistent.CtxKey, key), rpcTimeout)
	defer cancel()

	var hdr metadata.MD
	start := time.Now()
	_, err := client.Check(rpcCtx, &healthpb.HealthCheckRequest{}, grpc.Header(&hdr))
	latency := time.Since(start)
	if ctx.Err() != nil {
		// The recorder stopped mid-RPC; the result says nothing about the
		// balancer.
		return
	}

	s := sample{start: start.Sub(r.begin), latency: latency, key: string(key), err: err}
	if err == nil {
		if ids := hdr.Get(backendHeader); len(ids) > 0 {
			s.backend = ids[0]
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, s)
	r.latest[s.key] = s
}

func (r *recorder) stop() {
	r.cancel()
	r.wg.Wait()
}

// mark records a named point in time, such as the start of a fault, and
// returns it.
func (r *recorder) mark(name string) time.Duration {
	at := time.Since(r.begin)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event{at: at, name: name})
	return at
}

func (r *recorder) snapshot() []sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.samples)
}

// waitSteady waits until the latest RPC for every key succeeded on the
// backend that owns it on the full ring.
func (e *env) waitSteady(r *recorder) {
	e.t.Helper()
	require.EventuallyWithT(e.t, func(collect *assert.CollectT) {
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, k := range e.keys {
			s, ok := r.latest[string(k)]
			if !assert.True(collect, ok, "could not find latest") {
				return
			}
			if !assert.NoError(collect, s.err) {
				return
			}
			if !assert.Equal(collect, s.backend, e.home[string(k)], "selected backend did not match expected backend") {
				return
			}
		}
	}, 10*time.Second, 50*time.Millisecond, "keys never settled on their owners")
}

// window returns the samples for keys whose RPC started in [from, to).
func window(samples []sample, keys [][]byte, from, to time.Duration) []sample {
	want := make(map[string]bool, len(keys))
	for _, k := range keys {
		want[string(k)] = true
	}
	var out []sample
	for _, s := range samples {
		if want[s.key] && s.start >= from && s.start < to {
			out = append(out, s)
		}
	}
	return out
}

// settleTime returns how long after from each key took to reach its final
// placement: the end of its last RPC that failed, stalled, or went to a
// backend other than wantOwner. A stalled RPC succeeded on the right backend
// but waited on a connection first, so it still counts. It fails the test if
// a key has no correct RPC after that point, because then the key never
// settled.
func (e *env) settleTime(samples []sample, keys [][]byte, from, to time.Duration, wantOwner func([]byte) string) time.Duration {
	e.t.Helper()
	var worst time.Duration
	for _, k := range keys {
		want := wantOwner(k)
		ws := window(samples, [][]byte{k}, from, to)
		var settled time.Duration
		for _, s := range ws {
			if s.err != nil || s.backend != want || s.latency > stallThreshold {
				settled = max(settled, s.end()-from)
			}
		}
		require.True(e.t, slices.ContainsFunc(ws, func(s sample) bool {
			return s.start-from >= settled && s.err == nil && s.backend == want
		}), "key %s never settled on %s", k, want)
		worst = max(worst, settled)
	}
	return worst
}

// requireUndisturbed checks that every RPC for keys in [from, to) succeeded
// on the backend that owns the key on the full ring.
func (e *env) requireUndisturbed(samples []sample, keys [][]byte, from, to time.Duration) {
	e.t.Helper()
	ws := window(samples, keys, from, to)
	require.NotEmpty(e.t, ws)
	var bad []string
	for _, s := range ws {
		if want := e.home[s.key]; s.err != nil || s.backend != want {
			bad = append(bad, fmt.Sprintf("%s at %v: backend=%q want=%q err=%v", s.key, s.start, s.backend, want, s.err))
		}
	}
	require.Empty(e.t, bad, "%d of %d RPCs were disturbed", len(bad), len(ws))
}

// summarize logs error counts and latency percentiles for a window, and
// returns the number of failed RPCs.
func (e *env) summarize(label string, samples []sample) int {
	e.t.Helper()
	if len(samples) == 0 {
		e.t.Logf("%-28s no RPCs", label)
		return 0
	}
	latencies := make([]time.Duration, 0, len(samples))
	codes := map[string]int{}
	failed := 0
	for _, s := range samples {
		latencies = append(latencies, s.latency)
		if s.err != nil {
			failed++
			codes[status.Code(s.err).String()]++
		}
	}
	slices.Sort(latencies)
	pct := func(p float64) time.Duration {
		return latencies[int(p*float64(len(latencies)-1))].Round(100 * time.Microsecond)
	}
	e.t.Logf("%-28s rpcs=%-6d failed=%-5d %v p50=%v p99=%v max=%v",
		label, len(samples), failed, codes, pct(0.5), pct(0.99), latencies[len(latencies)-1].Round(time.Millisecond))
	return failed
}

// writeResults saves every sample and event as CSV under $E2E_RESULTS_DIR,
// if it is set, so a run can be plotted.
func (e *env) writeResults(r *recorder) {
	e.t.Helper()
	dir := os.Getenv("E2E_RESULTS_DIR")
	if dir == "" {
		return
	}
	require.NoError(e.t, os.MkdirAll(dir, 0o755))
	base := filepath.Join(dir, strings.ReplaceAll(e.t.Name(), "/", "_"))

	r.mu.Lock()
	defer r.mu.Unlock()

	writeCSV := func(path string, rows [][]string) {
		f, err := os.Create(path)
		require.NoError(e.t, err)
		w := csv.NewWriter(f)
		require.NoError(e.t, w.WriteAll(rows))
		require.NoError(e.t, f.Close())
	}

	rows := [][]string{{"start_ms", "latency_ms", "key", "owner", "backend", "code"}}
	for _, s := range r.samples {
		rows = append(rows, []string{
			strconv.FormatFloat(float64(s.start)/float64(time.Millisecond), 'f', 3, 64),
			strconv.FormatFloat(float64(s.latency)/float64(time.Millisecond), 'f', 3, 64),
			s.key,
			e.home[s.key],
			s.backend,
			status.Code(s.err).String(),
		})
	}
	writeCSV(base+".samples.csv", rows)

	rows = [][]string{{"at_ms", "event"}}
	for _, ev := range r.events {
		rows = append(rows, []string{strconv.FormatInt(ev.at.Milliseconds(), 10), ev.name})
	}
	writeCSV(base+".events.csv", rows)
	e.t.Logf("wrote %s.{samples,events}.csv", base)
}
