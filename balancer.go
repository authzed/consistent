// Package consistent implements a gRPC Balancer that routes requests based
// upon a consistent hashring.
//
// The hashing algorithm is customizable, but xxhash is recommended.
//
// A large portion of the structure of this library is based off of the example
// implementation in grpc-go. That original work is copyrighted by the gRPC
// authors and licensed under the Apache License, Version 2.0.
//
// This package relies on the synchronization guarantees provided by
// `ccBalancerWrapper`, an upstream type that serializes calls to the balancer.
package consistent

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	"github.com/authzed/consistent/hashring"
)

type ctxKey string

const (
	// BalancerName is the name used to identify this implementation of a
	// consistent-hashring balancer to gRPC.
	BalancerName = "consistent-hashring"

	// CtxKey is the key that will be present in each gRPC request's context
	// that points to the value that will be hashed in order to map the request
	// to the hashring.
	//
	// The value stored at this key must be []byte.
	CtxKey ctxKey = "requestKey"

	// DefaultReplicationFactor is the value that will be used when parsing a
	// service config provides an invalid value.
	DefaultReplicationFactor = 100

	// DefaultSpread is the value that will be used when parsing a service
	// config provides an invalid value.
	DefaultSpread = 1
)

// DefaultServiceConfigJSON is a helper to easily leverage the defaults.
//
// Here's an example:
// ```go
// grpc.Dial(addr, grpc.WithDefaultServiceConfig(consistent.DefaultServiceConfigJSON))
// ```
var DefaultServiceConfigJSON = (&BalancerConfig{
	ReplicationFactor: DefaultReplicationFactor,
	Spread:            DefaultSpread,
}).MustServiceConfigJSON()

// BalancerConfig exposes the configurable aspects of the balancer.
//
// This type is meant to be used with `grpc.WithDefaultServiceConfig()` through
// the `ServiceConfigJSON()` or `MustServiceConfigJSON()` methods.
//
// If you're unsure of what values to use in this configuration, use
// `DefaultBalancerConfig`.
type BalancerConfig struct {
	serviceconfig.LoadBalancingConfig `json:"-"`
	ReplicationFactor                 uint16 `json:"replicationFactor,omitempty"`
	Spread                            uint8  `json:"spread,omitempty"`
}

// ServiceConfigJSON encodes the current config into the gRPC Service Config
// JSON format.
func (c *BalancerConfig) ServiceConfigJSON() (string, error) {
	type wrapper struct {
		Config []map[string]*BalancerConfig `json:"loadBalancingConfig"`
	}

	out := wrapper{Config: []map[string]*BalancerConfig{{BalancerName: c}}}

	j, err := json.Marshal(out)
	if err != nil {
		return "", err
	}

	return string(j), nil
}

// MustServiceConfigJSON calls ServiceConfigJSON, but panics if there is an
// error.
func (c *BalancerConfig) MustServiceConfigJSON() string {
	o, err := c.ServiceConfigJSON()
	if err != nil {
		panic(err)
	}

	return o
}

var logger = grpclog.Component("consistenthashring")

// NewBuilder allocates a new gRPC balancer.Builder that will route traffic
// according to a hashring configured with the provided hash function.
//
// The following is an example usage:
// ```go
// balancer.Register(consistent.NewBuilder(xxhash.Sum64))
// ```
func NewBuilder(hashfn hashring.HashFunc) Builder {
	return &builder{hashfn: hashfn}
}

type subConnMember struct {
	balancer.SubConn
	key string
}

// Key implements hashring.Member.
// This value is what will be hashed for placement on the consistent hash ring.
func (s subConnMember) Key() string { return s.key }

var _ hashring.Member = (*subConnMember)(nil)

type builder struct {
	sync.Mutex
	hashfn hashring.HashFunc
	config BalancerConfig
}

// Builder combines both of gRPC's `balancer.Builder` and
// `balancer.ConfigParser` interfaces.
type Builder interface {
	balancer.Builder
	balancer.ConfigParser
}

var _ Builder = (*builder)(nil)

func (b *builder) Name() string { return BalancerName }

func (b *builder) Build(cc balancer.ClientConn, _ balancer.BuildOptions) balancer.Balancer {
	bal := &ringBalancer{
		cc:          cc,
		subConns:    resolver.NewAddressMapV2[any](),
		scStates:    make(map[balancer.SubConn]connectivity.State),
		scKeys:      make(map[balancer.SubConn]string),
		ringMembers: make(map[balancer.SubConn]struct{}),
		csEvltr:     &balancer.ConnectivityStateEvaluator{},
		state:       connectivity.Connecting,
		hasher:      b.hashfn,
		picker:      base.NewErrPicker(balancer.ErrNoSubConnAvailable),
	}

	return bal
}

func (b *builder) ParseConfig(js json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	var lbCfg BalancerConfig
	if err := json.Unmarshal(js, &lbCfg); err != nil {
		return nil, fmt.Errorf("wrr: unable to unmarshal LB policy config: %s, error: %w", string(js), err)
	}

	logger.Infof("parsed balancer config %s", js)

	if lbCfg.ReplicationFactor == 0 {
		lbCfg.ReplicationFactor = DefaultReplicationFactor
	}

	if lbCfg.Spread == 0 {
		lbCfg.Spread = DefaultSpread
	}

	b.Lock()
	b.config = lbCfg
	b.Unlock()

	return &lbCfg, nil
}

type ringBalancer struct {
	state    connectivity.State
	cc       balancer.ClientConn
	picker   balancer.Picker
	csEvltr  *balancer.ConnectivityStateEvaluator
	subConns *resolver.AddressMapV2[any]
	scStates map[balancer.SubConn]connectivity.State
	// scKeys holds the hashring key of every SubConn the resolver gave us.
	scKeys map[balancer.SubConn]string
	// ringMembers is the set of SubConns on the hashring: only READY ones.
	ringMembers map[balancer.SubConn]struct{}

	config   *BalancerConfig
	hashring *hashring.Ring
	hasher   hashring.HashFunc

	resolverErr error // the last error reported by the resolver; cleared on successful resolution
	connErr     error // the last connection error; cleared upon leaving TransientFailure
}

var _ balancer.Balancer = (*ringBalancer)(nil)

func (b *ringBalancer) ResolverError(err error) {
	b.resolverErr = err
	if b.subConns.Len() == 0 {
		b.state = connectivity.TransientFailure
		b.picker = base.NewErrPicker(errors.Join(b.connErr, b.resolverErr))
	}

	if b.state != connectivity.TransientFailure {
		// The picker will not change since the balancer does not currently
		// report an error.
		return
	}

	b.cc.UpdateState(balancer.State{
		ConnectivityState: b.state,
		Picker:            b.picker,
	})
}

// UpdateClientConnState is called when there are changes in the Address set or
// Service Config that the balancer may want to react to.
//
// In this case, the hashring is updated and a new picker using that hashring
// is generated.
func (b *ringBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	if logger.V(2) {
		logger.Info("got new ClientConn state: ", s)
	}
	// Successful resolution: clear resolver error and ensure we return nil.
	b.resolverErr = nil

	// update the service config if it has changed
	if s.BalancerConfig != nil {
		svcConfig := s.BalancerConfig.(*BalancerConfig)
		if b.config == nil || svcConfig.ReplicationFactor != b.config.ReplicationFactor {
			b.hashring = hashring.MustNew(b.hasher, svcConfig.ReplicationFactor)
			// The new ring starts empty: put every READY SubConn back on it.
			b.ringMembers = make(map[balancer.SubConn]struct{})
			for sc, st := range b.scStates {
				if st != connectivity.Ready {
					continue
				}
				if err := b.addToRing(sc); err != nil {
					return err
				}
			}
		}
		b.config = svcConfig
	}

	// if there's no hashring yet, the balancer hasn't yet parsed an initial
	// service config with settings
	if b.hashring == nil {
		b.picker = base.NewErrPicker(errors.Join(b.connErr, b.resolverErr))
		b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})

		return fmt.Errorf("no hashring configured")
	}

	// Look through the set of addresses the resolver has passed to the balancer
	// if any new targets have been added, they are added to the hashring, and
	// any that have been removed since the last update are removed from the
	// hashring.
	addrsSet := resolver.NewAddressMapV2[any]()
	for _, addr := range s.ResolverState.Addresses {
		addrsSet.Set(addr, nil)

		if _, ok := b.subConns.Get(addr); !ok {
			// addr is addr new address (not existing in b.subConns).
			// NewSubConn is deprecated only to warn that a SubConn will soon
			// hold a single address. We already pass exactly one address, and
			// grpc offers no replacement API yet.
			//nolint:staticcheck // SA1019: single-address usage is the future-proof form
			sc, err := b.cc.NewSubConn([]resolver.Address{addr}, balancer.NewSubConnOptions{HealthCheckEnabled: false})
			if err != nil {
				logger.Warningf("base.baseBalancer: failed to create new SubConn: %v", err)
				continue
			}

			b.subConns.Set(addr, sc)
			b.scStates[sc] = connectivity.Idle
			b.scKeys[sc] = addr.ServerName + addr.Addr
			b.csEvltr.RecordTransition(connectivity.Shutdown, connectivity.Idle)
			sc.Connect()
		}
	}

	for _, addr := range b.subConns.Keys() {
		sci, _ := b.subConns.Get(addr)
		sc := sci.(balancer.SubConn)
		// addr was removed by resolver.
		if _, ok := addrsSet.Get(addr); !ok {
			sc.Shutdown()
			b.subConns.Delete(addr)
			// Keep the state of this sc in b.scStates until sc's state becomes Shutdown.
			// The entry will be deleted in UpdateSubConnState.
			if err := b.removeFromRing(sc); err != nil {
				return err
			}
		}
	}

	if logger.V(2) {
		logger.Infof("%d hashring members found", len(b.hashring.Members()))

		for _, m := range b.hashring.Members() {
			logger.Infof("hashring member %s", m.Key())
		}
	}

	// If resolver state contains no addresses, return an error so ClientConn
	// will trigger re-resolve. Also records this as addr resolver error, so when
	// the overall state turns transient failure, the error message will have
	// the zero address information.
	if len(s.ResolverState.Addresses) == 0 {
		b.ResolverError(errors.New("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	b.regeneratePicker()
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})

	return nil
}

// regeneratePicker installs an error picker while the balancer is in
// TRANSIENT_FAILURE and a hashring picker otherwise.
func (b *ringBalancer) regeneratePicker() {
	if b.state == connectivity.TransientFailure {
		b.picker = base.NewErrPicker(errors.Join(b.connErr, b.resolverErr))
		return
	}

	b.picker = &picker{
		hashring: b.hashring,
		spread:   b.config.Spread,
	}
}

// addToRing places sc on the hashring if it is not already there.
func (b *ringBalancer) addToRing(sc balancer.SubConn) error {
	if _, ok := b.ringMembers[sc]; ok {
		return nil
	}

	if err := b.hashring.Add(subConnMember{SubConn: sc, key: b.scKeys[sc]}); err != nil {
		return fmt.Errorf("couldn't add to hashring: %w", err)
	}

	b.ringMembers[sc] = struct{}{}

	return nil
}

// removeFromRing takes sc off the hashring if it is there.
func (b *ringBalancer) removeFromRing(sc balancer.SubConn) error {
	if _, ok := b.ringMembers[sc]; !ok {
		return nil
	}

	if err := b.hashring.Remove(subConnMember{SubConn: sc, key: b.scKeys[sc]}); err != nil {
		return fmt.Errorf("couldn't remove from hashring: %w", err)
	}

	delete(b.ringMembers, sc)

	return nil
}

// UpdateSubConnState is called when there's a change in a subconnection state.
// Subconnection state can affect the overall state of the balancer.
// This also attempts to reconnect any idle connections.
func (b *ringBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	s := state.ConnectivityState
	if logger.V(2) {
		logger.Infof("base.baseBalancer: handle SubConn state change: %p, %v", sc, s)
	}

	oldS, ok := b.scStates[sc]
	if !ok {
		if logger.V(2) {
			logger.Infof("base.baseBalancer: got state changes for an unknown SubConn: %p, %v", sc, s)
		}

		return
	}

	if oldS == connectivity.TransientFailure &&
		(s == connectivity.Connecting || s == connectivity.Idle) {
		// Once a subconn enters TRANSIENT_FAILURE, ignore subsequent IDLE or
		// CONNECTING transitions to prevent the aggregated state from being
		// always CONNECTING when many backends exist but are all down.
		if s == connectivity.Idle {
			sc.Connect()
		}

		return
	}

	b.scStates[sc] = s

	// A backend only receives traffic while its connection is READY. Keys
	// hashed to a backend that is connecting or failed move to the closest
	// ready member instead of waiting on a connection that may never come.
	var ringErr error
	if s == connectivity.Ready {
		ringErr = b.addToRing(sc)
	} else {
		ringErr = b.removeFromRing(sc)
	}
	if ringErr != nil {
		logger.Warningf("consistent-hashring: %v", ringErr)
	}

	switch s {
	case connectivity.Idle:
		sc.Connect()
	case connectivity.Shutdown:
		// When an address was removed by resolver, b called RemoveSubConn but
		// kept the sc's state in scStates. Remove state for this sc here.
		delete(b.scStates, sc)
		delete(b.scKeys, sc)
	case connectivity.TransientFailure:
		// Save error to be reported via picker.
		b.connErr = state.ConnectionError
	}

	b.state = b.csEvltr.RecordTransition(oldS, s)
	b.regeneratePicker()

	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
}

func (b *ringBalancer) Close() {
	// No internal state to clean up and no need to call RemoveSubConn.
}

func (b *ringBalancer) ExitIdle() {
	// No-op as we already reconnect SubConns on demand when they report
	// connectivity.Idle in UpdateSubConnState. This is here to satisfy
	// the balancer.Balancer interface >v1.74.0
}

type picker struct {
	hashring *hashring.Ring
	spread   uint8
}

var _ balancer.Picker = (*picker)(nil)

// Pick returns a subconnection to use for a request based on the request info.
//
// The value stored in CtxKey is hashed into the hashring, and the resulting
// subconnection is used.
//
// The hashring only contains READY subconnections, so a key whose closest
// backend is down is served by the next closest ready backend. When no
// backend is ready the RPC is queued until one becomes ready (or the
// balancer reports TRANSIENT_FAILURE and installs an error picker).
//
// Spread can be increased to be robust against single node availability
// problems. If spread is greater than 1, a random selection is made from the
// set of subconns matching the hash.
func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	key := info.Ctx.Value(CtxKey).([]byte)

	members, err := p.hashring.FindN(key, p.spread)
	if errors.Is(err, hashring.ErrNotEnoughMembers) {
		// Fewer ready backends than the configured spread: use those that are.
		members, err = p.hashring.FindN(key, 1)
	}
	if errors.Is(err, hashring.ErrNotEnoughMembers) {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	if err != nil {
		return balancer.PickResult{}, err
	}

	index := 0
	if len(members) > 1 {
		index = intn(uint8(len(members)))
	}

	chosen := members[index].(subConnMember)

	return balancer.PickResult{SubConn: chosen.SubConn}, nil
}

// intn returns, as an int, a non-negative pseudo-random number in the
// half-open interval [0,n).
//
// Under the hood, it's taking advantage of maphash's use of runtime.fastrand
// for an extremely fast, thread-safe PRNG.
var intn = func(n uint8) int {
	out := int(new(maphash.Hash).Sum64())
	if out < 0 {
		out = -out
	}
	return out % int(n)
}
