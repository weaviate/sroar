package sroar

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// encode places `high` in the upper (64-k) bits and `low` in the lower k bits.
func encode(high, low uint64, k uint) uint64 { return (high << k) | low }

// bitmapOf is a small constructor so test bodies read as one expression per
// bitmap instead of a Set loop.
func bitmapOf(values ...uint64) *Bitmap {
	bm := NewBitmap()
	bm.SetMany(values)
	return bm
}

func TestFloorMasked(t *testing.T) {
	// Most cases use this mask: each container key is its own group.
	// With mask & 0xFFFF == 0 the per-value step degenerates — every bit
	// in vals that shares its container-key group with any candidates
	// container will only emit when the bit's exact value (= same low
	// 16 bits) is set in some matching candidates container. The
	// surviving abstract cases are the ones whose expected output
	// coincides with that membership semantic.
	const mh = uint64(0xFFFFFFFFFFFF0000)

	t.Run("empty vals", func(t *testing.T) {
		res := FloorMasked(NewBitmap(), bitmapOf(encode(1, 50, 16)), mh)
		require.Empty(t, res.ToArray())
	})

	t.Run("empty candidates", func(t *testing.T) {
		res := FloorMasked(bitmapOf(encode(1, 100, 16)), NewBitmap(), mh)
		require.Empty(t, res.ToArray())
	})

	t.Run("no floor in group", func(t *testing.T) {
		vals := bitmapOf(encode(1, 50, 16))
		candidates := bitmapOf(encode(1, 100, 16))
		require.Empty(t, FloorMasked(vals, candidates, mh).ToArray())
	})

	t.Run("value is its own floor (value in candidates)", func(t *testing.T) {
		vals := bitmapOf(encode(1, 100, 16))
		candidates := bitmapOf(encode(1, 100, 16))
		require.Equal(t, []uint64{encode(1, 100, 16)}, FloorMasked(vals, candidates, mh).ToArray())
	})

	t.Run("group isolation: numerically valid but wrong group", func(t *testing.T) {
		// candidates' only bit is numerically smaller than vals', but in a
		// different masked group → must not contribute.
		vals := bitmapOf(encode(2, 100, 16))
		candidates := bitmapOf(encode(1, 50, 16))
		require.Empty(t, FloorMasked(vals, candidates, mh).ToArray())
	})

	t.Run("mask all-ones: membership regime, result is vals intersect candidates", func(t *testing.T) {
		// With mask & 0xFFFF == 0xFFFF (here: all-ones), the per-value
		// step is a membership test: a bit emits iff it's also in
		// candidates (in its container-key group). So FloorMasked
		// degenerates to vals ∩ candidates.
		vals := bitmapOf(encode(1, 100, 16), encode(2, 200, 16), encode(3, 300, 16))
		candidates := bitmapOf(encode(2, 200, 16), encode(4, 400, 16))
		expected := []uint64{encode(2, 200, 16)}
		require.Equal(t, expected, FloorMasked(vals, candidates, 0xFFFFFFFFFFFFFFFF).ToArray())
	})

	t.Run("idempotence: FloorMasked(x, x, mask) == x (set-equal)", func(t *testing.T) {
		// Every bit in x is trivially a member of x, so the membership
		// test emits it at its own container key. Result equals x
		// set-wise.
		x := NewBitmap()
		for i := uint64(0); i < 200; i++ {
			x.Set(encode(i%17, i*13, 16))
		}
		got := FloorMasked(x, x, mh).ToArray()
		require.Equal(t, x.ToArray(), got)
	})

	t.Run("regression: same destination key flushed twice in one group", func(t *testing.T) {
		// Under the membership semantic, different low values within a
		// single vals container can resolve to different candidates
		// entries — and a later vals container can re-resolve to an
		// earlier candidates entry. The resulting destination-key emit
		// sequence within one processFloorGroup call is non-monotonic
		// (here: A, B, A). Under a historical single-accumulator design
		// this triggered an OR-merge that orphaned the first flush's
		// bit; the current per-bucket design dedups naturally.
		//
		// Verified trace (one masked group, mask = 0xFFFF):
		//   vals container @0x3000000000 = {0x000A, 0x0014}
		//     0x000A → cache hit at candidates @0x2000000000 → emit 0x200000000A
		//     0x0014 → cache hit at candidates @0x1000000000 → emit 0x1000000014
		//   vals container @0x4000000000 = {0x0018}
		//     0x0018 → cache hit at candidates @0x2000000000 → emit 0x2000000018
		// Destination-key sequence: A=0x2000000000, B=0x1000000000, A=0x2000000000.
		vals := bitmapOf(0x300000000A, 0x3000000014, 0x4000000018)
		candidates := bitmapOf(0x1000000014, 0x200000000A, 0x2000000018)
		// ToArray returns sorted ascending; reorder accordingly.
		expected := []uint64{0x1000000014, 0x200000000A, 0x2000000018}
		require.Equal(t, expected, FloorMasked(vals, candidates, 0xFFFF).ToArray())
	})

	t.Run("regression: match more than one candidates container behind cursor", func(t *testing.T) {
		// All four containers collapse into one masked group (mask &
		// 0xFFFFFFFFFFFF0000 == 0). The membership regime applies
		// because mask & 0xFFFF == 0xFFFF.
		//
		// vals has one bit with container key 0x7000000000 and low 0x14.
		// candidates has three containers in this group:
		//   c[0] at 0x2000000000 with {0x14}  ← only match for low 0x14
		//   c[1] at 0x5000000000 with {0x0A}
		//   c[2] at 0x6000000000 with {0x0A}
		//
		// All three are ≤ the vals key, so the cursor advances past all of
		// them. An earlier impl tracked only the current candidates container
		// and one step back, which left c[2] and c[1] in scope at emit
		// time — both missing the match. The correct floor is c[0]'s
		// 0x2000000014, which a backward walk over the group finds.
		vals := bitmapOf(0x7000000014)
		candidates := bitmapOf(0x2000000014, 0x500000000A, 0x600000000A)
		expected := []uint64{0x2000000014}
		require.Equal(t, expected, FloorMasked(vals, candidates, 0xFFFF).ToArray())
	})
}

// TestFloorMaskedFixtures covers shape-based fixtures with a non-contiguous
// mask. Encoding: position = (level << 36) | groupID. mask = 0x0000000FFFFFFFFF
// groups by groupID: low 36 bits survive, top 28 are zeroed.
//
// Kept separate from TestFloorMasked because the mask shape is genuinely
// different — after the impl's internal mask &= 0xFFFFFFFFFFFF0000, the
// effective mask is 0x0000000FFFF0000 (bits 16..35 set), a non-contiguous
// mask whose masked-key order does NOT follow raw-key order. These cases
// exercise that path directly.
func TestFloorMaskedFixtures(t *testing.T) {
	const mask = uint64(0x0000000FFFFFFFFF)

	cases := []struct {
		name       string
		vals       []uint64
		candidates []uint64
		want       []uint64
	}{
		// --- Single group (groupID = 10) ---
		{
			"three values mapping to two floors, single group",
			[]uint64{0x300000000A, 0x400000000A, 0x600000000A},
			[]uint64{0x200000000A, 0x500000000A},
			[]uint64{0x200000000A, 0x500000000A},
		},
		{
			"three values mapping to one far-below floor, single group",
			[]uint64{0x300000000A, 0x400000000A, 0x600000000A},
			[]uint64{0x100000000A},
			[]uint64{0x100000000A},
		},
		{
			"two values mapping to one far-below floor, single group",
			[]uint64{0x200000000A, 0x500000000A},
			[]uint64{0x100000000A},
			[]uint64{0x100000000A},
		},
		{
			"single value picks lower of two candidates",
			[]uint64{0x300000000A},
			[]uint64{0x200000000A, 0x500000000A},
			[]uint64{0x200000000A},
		},
		{
			"single value picks higher of two candidates",
			[]uint64{0x600000000A},
			[]uint64{0x200000000A, 0x500000000A},
			[]uint64{0x500000000A},
		},

		// --- Multi group ---
		{
			// ToArray returns sorted ascending; expected reordered to match.
			"two groups, each with multiple values and floors",
			[]uint64{0x300000000A, 0x600000000A, 0x3000000014},
			[]uint64{0x200000000A, 0x500000000A, 0x2000000014},
			[]uint64{0x200000000A, 0x2000000014, 0x500000000A},
		},
		{
			"two vals groups, only one has matching candidates",
			[]uint64{0x300000000A, 0x3000000014},
			[]uint64{0x200000000A},
			[]uint64{0x200000000A},
		},

		// --- Multi-root within one group (groupID = 30) ---
		{
			"two floors same group, value at each picks its own",
			[]uint64{0x200000001E, 0x500000001E},
			[]uint64{0x100000001E, 0x400000001E},
			[]uint64{0x100000001E, 0x400000001E},
		},
		{
			"two value-levels above two floor roots, both lift fully",
			[]uint64{0x300000001E, 0x600000001E},
			[]uint64{0x100000001E, 0x400000001E},
			[]uint64{0x100000001E, 0x400000001E},
		},
		{
			"two values, two floors, single group",
			[]uint64{0x300000001E, 0x600000001E},
			[]uint64{0x200000001E, 0x500000001E},
			[]uint64{0x200000001E, 0x500000001E},
		},

		// --- Edge cases ---
		{
			"value below the only floor in the group",
			[]uint64{0x100000000A},
			[]uint64{0x200000000A},
			nil,
		},
		{
			"idempotence",
			[]uint64{0x200000000A, 0x500000000A},
			[]uint64{0x200000000A, 0x500000000A},
			[]uint64{0x200000000A, 0x500000000A},
		},
		{
			// The value is numerically larger than a smaller candidate
			// from a different group, but the cross-group candidate must
			// be rejected on group grounds.
			"cross-group isolation (numerically tempting)",
			[]uint64{0x3000000014},
			[]uint64{0x100000000A, 0x1000000014},
			[]uint64{0x1000000014},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vals := bitmapOf(tc.vals...)
			candidates := bitmapOf(tc.candidates...)
			got := FloorMasked(vals, candidates, mask).ToArray()
			if tc.want == nil {
				require.Empty(t, got)
				return
			}
			require.Equal(t, tc.want, got)
		})
	}
}

// TestFloorMaskedToBufAllocs measures the algorithm's allocation count under
// a pre-sized buffer. Each call currently allocates: the *Bitmap header that
// NewBitmapToBuf returns, two entries slices from buildKeyedMaskedEntries
// (one per input — necessary for correctness when the mask isn't contiguous-
// high; see comments in floorMaskedInto), and the two bucket storage slices
// (bucketBufs + bucketStates) used for the per-bucket flush. That's 5
// allocs/op.
//
// Reducing further would require pooling the bucket storage. Tracking the
// actual count makes accidental regressions visible.
func TestFloorMaskedToBufAllocs(t *testing.T) {
	rnd := rand.New(rand.NewSource(20260619))
	vals := NewBitmap()
	candidates := NewBitmap()
	for i := 0; i < 10_000; i++ {
		g := uint64(rnd.Intn(100))
		o := uint64(rnd.Intn(1 << 16))
		vals.Set((g << 16) | o)
		candidates.Set((g << 16) | o>>1)
	}
	const m = uint64(0xFFFFFFFFFFFF0000)

	buf := make([]byte, 1<<22) // 4MB — comfortably exceeds the result size

	// Pre-warm any one-time setup.
	_ = FloorMaskedToBuf(vals, candidates, m, buf)

	allocs := testing.AllocsPerRun(50, func() {
		_ = FloorMaskedToBuf(vals, candidates, m, buf)
	})
	t.Logf("FloorMaskedToBuf allocs/op = %.2f", allocs)
	require.LessOrEqual(t, allocs, 5.0,
		"expected ≤5 allocs/op (Bitmap header + 2 entries slices + bucketBufs + bucketStates); got %.2f", allocs)
}
