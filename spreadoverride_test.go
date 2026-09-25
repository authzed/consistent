package consistent

import (
	"context"
	"hash/maphash"
	"reflect"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/balancer"

	"github.com/authzed/consistent/hashring"
)

// pinIntn overrides the picker's random member selection with a version that
// uses a stable seed. Spread-based picks are then deterministic in tests.
func pinIntn(t *testing.T) {
	t.Helper()
	realIntn := intn
	t.Cleanup(func() { intn = realIntn })
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
}

// threeMemberPicker returns a picker with the given config spread over
// members "1", "2", "3". This is the same fixture as
// TestConsistentHashringPickerPick: key "test" picks member 1 at spread 1
// and member 3 at spread 2.
func threeMemberPicker(t *testing.T, spread uint8) *picker {
	t.Helper()
	p := &picker{
		hashring: hashring.MustNew(xxhash.Sum64, 100),
		spread:   spread,
	}
	require.NoError(t, p.hashring.Add(subConnMember{key: "1", SubConn: &fakeSubConn{id: "1"}}))
	require.NoError(t, p.hashring.Add(subConnMember{key: "2", SubConn: &fakeSubConn{id: "2"}}))
	require.NoError(t, p.hashring.Add(subConnMember{key: "3", SubConn: &fakeSubConn{id: "3"}}))
	return p
}

func pickKey(t *testing.T, p *picker, ctx context.Context) *fakeSubConn {
	t.Helper()
	got, err := p.Pick(balancer.PickInfo{Ctx: ctx})
	require.NoError(t, err)
	return got.SubConn.(*fakeSubConn)
}

// The picker uses the spread carried at SpreadCtxKey instead of the spread
// from the balancer config.
func TestPickerSpreadOverride(t *testing.T) {
	pinIntn(t)
	p := threeMemberPicker(t, 1)

	ctx := context.WithValue(context.Background(), CtxKey, []byte("test"))
	require.Equal(t, "1", pickKey(t, p, ctx).id, "sanity: config spread 1 picks member 1")

	ctx = context.WithValue(ctx, SpreadCtxKey, uint8(2))
	require.Equal(t, "3", pickKey(t, p, ctx).id,
		"an override of 2 must pick as the spread-2 config would")
}

// The picker ignores an override of zero and uses the config spread.
func TestPickerSpreadOverrideZeroIgnored(t *testing.T) {
	pinIntn(t)
	p := threeMemberPicker(t, 1)

	ctx := context.WithValue(context.Background(), CtxKey, []byte("test"))
	ctx = context.WithValue(ctx, SpreadCtxKey, uint8(0))
	require.Equal(t, "1", pickKey(t, p, ctx).id)
}

// A pick against a ring with no ready members queues the RPC instead of
// panicking: FindN(key, 0) succeeds with an empty result.
func TestPickerEmptyRingQueuesRPC(t *testing.T) {
	p := &picker{
		hashring: hashring.MustNew(xxhash.Sum64, 100),
		spread:   1,
	}

	ctx := context.WithValue(context.Background(), CtxKey, []byte("test"))
	ctx = context.WithValue(ctx, SpreadCtxKey, uint8(5))
	_, err := p.Pick(balancer.PickInfo{Ctx: ctx})
	require.ErrorIs(t, err, balancer.ErrNoSubConnAvailable)
}

// A spread larger than the count of ready members uses all of them. The pick
// must not collapse to a single member.
func TestPickerSpreadClampedToMembership(t *testing.T) {
	pinIntn(t)
	p := threeMemberPicker(t, 1)

	ctx := context.WithValue(context.Background(), CtxKey, []byte("test"))
	ctx = context.WithValue(ctx, SpreadCtxKey, uint8(5))

	// The picked member must be one of the three that FindN returns over the
	// full membership.
	want, err := p.hashring.FindN([]byte("test"), 3)
	require.NoError(t, err)

	got := pickKey(t, p, ctx)
	found := false
	for _, m := range want {
		if m.(subConnMember).SubConn.(*fakeSubConn).id == got.id {
			found = true
		}
	}
	require.True(t, found, "picked member %q must be on the ring", got.id)
}
