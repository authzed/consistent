package consistent

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"

	"github.com/authzed/consistent/hashring"
)

type fakeSubConn struct {
	balancer.SubConn
	id string
}

func (fakeSubConn) Connect() {}

func keys(members []hashring.Member) []string {
	keys := make([]string, 0, len(members))
	for _, member := range members {
		keys = append(keys, member.Key())
	}
	return keys
}

// Note: this is testing picker behavior and not the hashring
// behavior itself, see `pkg/consistent` for tests of the hashring.
func TestConsistentHashringPickerPick(t *testing.T) {
	// Override the intn function with one that uses a stable seed.
	intn = func(n uint8) int {
		h := new(maphash.Hash)

		// This hack sets an unexported field using reflection.
		var seed maphash.Seed
		field := reflect.ValueOf(&seed).Elem().Field(0)
		unsafeField := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
		unsafeField.SetUint(uint64(1))
		h.SetSeed(seed)

		out := int(h.Sum64())
		if out < 0 {
			out = -out
		}
		return out % int(n)
	}

	tests := []struct {
		name   string
		spread uint8
		rf     uint16
		info   balancer.PickInfo
		want   balancer.PickResult
	}{
		{
			name:   "pick one",
			spread: 1,
			rf:     100,
			info: balancer.PickInfo{
				Ctx: context.WithValue(context.Background(), CtxKey, []byte("test")),
			},
			want: balancer.PickResult{
				SubConn: &fakeSubConn{id: "1"},
			},
		},
		{
			name:   "pick another",
			spread: 1,
			rf:     100,
			info: balancer.PickInfo{
				Ctx: context.WithValue(context.Background(), CtxKey, []byte("test2")),
			},
			want: balancer.PickResult{
				SubConn: &fakeSubConn{id: "3"},
			},
		},
		{
			name:   "pick with spread",
			spread: 2,
			rf:     100,
			info: balancer.PickInfo{
				Ctx: context.WithValue(context.Background(), CtxKey, []byte("test")),
			},
			want: balancer.PickResult{
				// without spread, this would always be 1.
				// it can be 1 or 3 with spread 2, but pinning the seed makes it always 3 in the test
				SubConn: &fakeSubConn{id: "3"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &picker{
				hashring: hashring.MustNewHashring(xxhash.Sum64, tt.rf),
				spread:   tt.spread,
			}
			require.NoError(t, p.hashring.Add(subConnMember{key: "1", SubConn: &fakeSubConn{id: "1"}}))
			require.NoError(t, p.hashring.Add(subConnMember{key: "2", SubConn: &fakeSubConn{id: "2"}}))
			require.NoError(t, p.hashring.Add(subConnMember{key: "3", SubConn: &fakeSubConn{id: "3"}}))

			got, err := p.Pick(tt.info)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestConsistentHashringBalancerConfigServiceConfigJSON(t *testing.T) {
	tests := []struct {
		name              string
		replicationFactor uint16
		spread            uint8
		want              string
	}{
		{
			name:              "sets rf and spread",
			replicationFactor: 300,
			spread:            2,
			want:              `{"loadBalancingConfig":[{"consistent-hashring":{"replicationFactor":300,"spread":2}}]}`,
		},
		{
			name:              "sets rf",
			replicationFactor: 300,
			want:              `{"loadBalancingConfig":[{"consistent-hashring":{"replicationFactor":300}}]}`,
		},
		{
			name:   "sets spread",
			spread: 1,
			want:   `{"loadBalancingConfig":[{"consistent-hashring":{"spread":1}}]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &BalancerConfig{
				ReplicationFactor: tt.replicationFactor,
				Spread:            tt.spread,
			}

			got, err := c.ServiceConfigJSON()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestConsistentHashringBalancerUpdateClientConnState(t *testing.T) {
	type balancerState struct {
		ConnectivityState connectivity.State
		err               error
		memberKeys        []string
		spread            uint8
		replicationFactor uint16
	}

	tests := []struct {
		name              string
		s                 []balancer.ClientConnState
		expectedStates    []balancerState
		expectedConnState connectivity.State
		wantErr           bool
	}{
		{
			name:              "no hashring",
			expectedStates:    []balancerState{},
			expectedConnState: connectivity.TransientFailure,
			wantErr:           true,
		},
		{
			name: "configures hashring, no addresses",
			s: []balancer.ClientConnState{{
				ResolverState: resolver.State{},
				BalancerConfig: &BalancerConfig{
					ReplicationFactor: 100,
					Spread:            1,
				},
			}},
			expectedStates: []balancerState{
				{
					ConnectivityState: connectivity.TransientFailure,
					err:               errors.Join(nil, fmt.Errorf("produced zero addresses")),
				},
			},
			expectedConnState: connectivity.TransientFailure,
			wantErr:           true,
		},
		{
			name: "configures hashring, 3 addresses",
			s: []balancer.ClientConnState{{
				ResolverState: resolver.State{
					Addresses: []resolver.Address{
						{ServerName: "t", Addr: "1"},
						{ServerName: "t", Addr: "2"},
						{ServerName: "t", Addr: "3"},
					},
				},
				BalancerConfig: &BalancerConfig{
					ReplicationFactor: 100,
					Spread:            1,
				},
			}},
			expectedStates: []balancerState{
				{
					ConnectivityState: connectivity.Connecting,
					memberKeys:        []string{"t1", "t2", "t3"},
					replicationFactor: 100,
					spread:            1,
				},
			},
			expectedConnState: connectivity.Idle,
		},
		{
			name: "existing hashring with 3 nodes, 1 removed",
			s: []balancer.ClientConnState{{
				ResolverState: resolver.State{
					Addresses: []resolver.Address{
						{ServerName: "t", Addr: "1"},
						{ServerName: "t", Addr: "2"},
						{ServerName: "t", Addr: "3"},
					},
				},
				BalancerConfig: &BalancerConfig{
					ReplicationFactor: 100,
					Spread:            1,
				},
			}, {
				ResolverState: resolver.State{
					Addresses: []resolver.Address{
						{ServerName: "t", Addr: "1"},
						{ServerName: "t", Addr: "2"},
					},
				},
			}},
			expectedStates: []balancerState{
				{
					ConnectivityState: connectivity.Connecting,
					memberKeys:        []string{"t1", "t2", "t3"},
					replicationFactor: 100,
					spread:            1,
				},
				{
					ConnectivityState: connectivity.Connecting,
					memberKeys:        []string{"t1", "t2"},
					replicationFactor: 100,
					spread:            1,
				},
			},
			expectedConnState: connectivity.Idle,
		},
		{
			name: "existing hashring with 3 nodes, 1 added",
			s: []balancer.ClientConnState{{
				ResolverState: resolver.State{
					Addresses: []resolver.Address{
						{ServerName: "t", Addr: "1"},
						{ServerName: "t", Addr: "2"},
						{ServerName: "t", Addr: "3"},
					},
				},
				BalancerConfig: &BalancerConfig{
					ReplicationFactor: 100,
					Spread:            1,
				},
			}, {
				ResolverState: resolver.State{
					Addresses: []resolver.Address{
						{ServerName: "t", Addr: "1"},
						{ServerName: "t", Addr: "2"},
						{ServerName: "t", Addr: "3"},
						{ServerName: "t", Addr: "4"},
					},
				},
			}},
			expectedStates: []balancerState{
				{
					ConnectivityState: connectivity.Connecting,
					memberKeys:        []string{"t1", "t2", "t3"},
					replicationFactor: 100,
					spread:            1,
				},
				{
					ConnectivityState: connectivity.Connecting,
					memberKeys:        []string{"t1", "t2", "t3", "t4"},
					replicationFactor: 100,
					spread:            1,
				},
			},
			expectedConnState: connectivity.Idle,
		},
		{
			name: "existing hashring with 3 nodes, 1 replaced",
			s: []balancer.ClientConnState{{
				ResolverState: resolver.State{
					Addresses: []resolver.Address{
						{ServerName: "t", Addr: "1"},
						{ServerName: "t", Addr: "2"},
						{ServerName: "t", Addr: "3"},
					},
				},
				BalancerConfig: &BalancerConfig{
					ReplicationFactor: 100,
					Spread:            1,
				},
			}, {
				ResolverState: resolver.State{
					Addresses: []resolver.Address{
						{ServerName: "t", Addr: "1"},
						{ServerName: "t", Addr: "2"},
						{ServerName: "t", Addr: "4"},
					},
				},
			}},
			expectedStates: []balancerState{
				{
					ConnectivityState: connectivity.Connecting,
					memberKeys:        []string{"t1", "t2", "t3"},
					replicationFactor: 100,
					spread:            1,
				},
				{
					ConnectivityState: connectivity.Connecting,
					memberKeys:        []string{"t1", "t2", "t4"},
					replicationFactor: 100,
					spread:            1,
				},
			},
			expectedConnState: connectivity.Idle,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBuilder(xxhash.Sum64)
			cc := newFakeClientConn()
			bb := b.Build(cc, balancer.BuildOptions{})
			cb := bb.(*ringBalancer)

			tt := tt

			done := make(chan struct{})

			go func() {
				i := 0

				if len(tt.expectedStates) == 0 {
					done <- struct{}{}
					return
				}

				for {
					s := <-cc.stateCh
					expected := tt.expectedStates[i]
					require.Equal(t, expected.ConnectivityState, s.ConnectivityState)

					if expected.err != nil {
						require.Equal(t, base.NewErrPicker(expected.err), s.Picker)
					} else {
						p := s.Picker.(*picker)
						require.Equal(t, expected.spread, p.spread)
						require.ElementsMatch(t, expected.memberKeys, keys(p.hashring.Members()))
					}

					i++
					done <- struct{}{}
				}
			}()

			for _, state := range tt.s {
				if err := cb.UpdateClientConnState(state); (err != nil) != tt.wantErr {
					t.Errorf("UpdateClientConnState() error = %v, wantErr %v", err, tt.wantErr)
				}

				<-done
			}

			require.Equal(t, tt.expectedConnState, cb.csEvltr.CurrentState())
		})
	}
}

type fakeClientConn struct {
	balancer.ClientConn

	stateCh chan balancer.State

	mu       sync.Mutex
	subConns map[balancer.SubConn]resolver.Address
}

func newFakeClientConn() *fakeClientConn {
	return &fakeClientConn{
		subConns: make(map[balancer.SubConn]resolver.Address),
		stateCh:  make(chan balancer.State),
	}
}

func (c *fakeClientConn) NewSubConn(addrs []resolver.Address, _ balancer.NewSubConnOptions) (balancer.SubConn, error) {
	sc := &fakeSubConn{}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.subConns[sc] = addrs[0]

	return sc, nil
}

func (c *fakeClientConn) RemoveSubConn(sc balancer.SubConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subConns, sc)
}

func (c *fakeClientConn) UpdateState(s balancer.State) {
	c.stateCh <- s
}
