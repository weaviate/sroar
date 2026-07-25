package sroar

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/require"
)

// countLazyContainers reports how many of bm's containers carry a deferred
// cardinality header. Tests use it to prove the lazy path actually ran, so a
// parity assertion cannot pass by never taking it.
func countLazyContainers(bm *Bitmap) int {
	n := 0
	for i, keys := 0, bm.keys.numKeys(); i < keys; i++ {
		if getCardinality(bm.data[bm.keys.val(i):]) == invalidCardinality {
			n++
		}
	}
	return n
}

// settled returns a copy of bm with every cardinality header written out
// exactly. It is the reference every observation is compared against: the two
// bitmaps hold identical bits, so any disagreement is a header being read
// without being settled first.
func settled(bm *Bitmap) *Bitmap {
	cp := make([]byte, bm.LenInBytes())
	copy(cp, toByteSlice(bm.data))
	out := FromBuffer(cp)
	out.reconcileCardinality()
	return out
}

func bitmapOf(vals ...uint64) *Bitmap {
	bm := NewBitmap()
	bm.SetMany(vals)
	return bm
}

// denseContainer returns a bitmap holding fill values inside the single
// container at key, spread so the container is bitmap-typed.
func denseContainer(key uint64, fill int, seed int64) *Bitmap {
	rnd := rand.New(rand.NewSource(seed))
	bm := NewBitmap()
	seen := make(map[uint16]struct{}, fill)
	for len(seen) < fill {
		v := uint16(rnd.Intn(maxCardinality))
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		bm.Set(key | uint64(v))
	}
	return bm
}

func fullContainer(key uint64) *Bitmap {
	bm := NewBitmap()
	for i := 0; i < maxCardinality; i++ {
		bm.Set(key | uint64(i))
	}
	return bm
}

// TestLazyCardinality_HeaderStates pins the three header values the deferred
// path may produce. Empty and full have to stay exact because the next
// operation branches on them: bitmap.andBitmapAlt short-circuits on
// bnum == 0 and bitmap.orBitmapAlt skips a full destination outright.
func TestLazyCardinality_HeaderStates(t *testing.T) {
	const key = uint64(0)

	t.Run("full or full stays exactly full", func(t *testing.T) {
		a, b := fullContainer(key), fullContainer(key)
		a.OrConc(b, 1)
		require.Equal(t, 0, countLazyContainers(a))
		require.Equal(t, maxCardinality, getCardinality(a.getContainer(a.keys.val(0))))
	})

	t.Run("or reaching full is detected", func(t *testing.T) {
		a, b := NewBitmap(), NewBitmap()
		for i := 0; i < maxCardinality; i++ {
			if i%2 == 0 {
				a.Set(key | uint64(i))
			} else {
				b.Set(key | uint64(i))
			}
		}
		a.ConvertToBitmapContainers()
		b.ConvertToBitmapContainers()
		a.OrConc(b, 1)
		require.Equal(t, 0, countLazyContainers(a))
		require.Equal(t, maxCardinality, a.GetCardinality())
	})

	t.Run("and to empty stays exactly zero", func(t *testing.T) {
		a, b := NewBitmap(), NewBitmap()
		for i := 0; i < 6000; i++ {
			a.Set(key | uint64(2*i))
			b.Set(key | uint64(2*i+1))
		}
		a.ConvertToBitmapContainers()
		b.ConvertToBitmapContainers()
		a.AndConc(b, 1)
		require.Equal(t, 0, countLazyContainers(a))
		require.True(t, a.IsEmpty())
		require.Equal(t, 0, a.GetCardinality())
	})

	t.Run("partial result defers", func(t *testing.T) {
		a := denseContainer(key, 20000, 1)
		b := denseContainer(key, 20000, 2)
		a.ConvertToBitmapContainers()
		b.ConvertToBitmapContainers()
		a.OrConc(b, 1)
		require.Equal(t, 1, countLazyContainers(a))
		require.Equal(t, settled(a).GetCardinality(), a.GetCardinality())
	})
}

// observation is one way a caller can read a bitmap. Every entry has to give
// the same answer on a bitmap holding deferred headers as on the same bits with
// the headers written out.
type observation struct {
	name string
	read func(t *testing.T, bm *Bitmap) any
}

func observations() []observation {
	return []observation{
		{"GetCardinality", func(_ *testing.T, bm *Bitmap) any { return bm.GetCardinality() }},
		{"IsEmpty", func(_ *testing.T, bm *Bitmap) any { return bm.IsEmpty() }},
		{"NumContainers", func(_ *testing.T, bm *Bitmap) any { return bm.NumContainers() }},
		{"ToArray", func(_ *testing.T, bm *Bitmap) any { return bm.ToArray() }},
		{"Minimum", func(_ *testing.T, bm *Bitmap) any { return bm.Minimum() }},
		{"Maximum", func(_ *testing.T, bm *Bitmap) any { return bm.Maximum() }},
		{"String", func(_ *testing.T, bm *Bitmap) any { return bm.String() }},
		{"Select", func(t *testing.T, bm *Bitmap) any {
			out := []uint64{}
			n := bm.GetCardinality()
			for _, i := range []int{0, 1, n / 3, n / 2, n - 1} {
				if i < 0 || i >= n {
					continue
				}
				v, err := bm.Select(uint64(i))
				require.NoError(t, err)
				out = append(out, v)
			}
			// out of range must report an error rather than panic
			_, err := bm.Select(uint64(n))
			require.Error(t, err)
			return out
		}},
		{"Rank", func(_ *testing.T, bm *Bitmap) any {
			out := []int{}
			for _, v := range bm.ToArray() {
				out = append(out, bm.Rank(v))
			}
			return out
		}},
		{"Contains", func(_ *testing.T, bm *Bitmap) any {
			out := []bool{}
			for v := uint64(0); v < 4096; v++ {
				out = append(out, bm.Contains(v))
			}
			return out
		}},
		{"Iterator", func(_ *testing.T, bm *Bitmap) any {
			out := []uint64{}
			it := bm.NewIterator()
			for v := it.Next(); v != 0; v = it.Next() {
				out = append(out, v)
			}
			return out
		}},
		{"ManyIterator", func(_ *testing.T, bm *Bitmap) any {
			out := []uint64{}
			it := bm.ManyIterator()
			buf := make([]uint64, 97)
			for n := it.NextMany(buf); n > 0; n = it.NextMany(buf) {
				out = append(out, buf[:n]...)
			}
			return out
		}},
		{"ContainsCursor", func(_ *testing.T, bm *Bitmap) any {
			out := []bool{}
			cur := bm.NewContainsCursor()
			for v := uint64(0); v < 4096; v++ {
				out = append(out, cur.Contains(v))
			}
			return out
		}},
		{"NextGeq", func(_ *testing.T, bm *Bitmap) any {
			type hit struct {
				V  uint64
				Ok bool
			}
			out := []hit{}
			cur := bm.NewContainsCursor()
			for v := uint64(0); v < 4096; v += 7 {
				got, ok := cur.NextGeq(v)
				out = append(out, hit{got, ok})
			}
			return out
		}},
		{"Clone", func(_ *testing.T, bm *Bitmap) any { return bm.Clone().ToArray() }},
		{"CloneToBuf", func(_ *testing.T, bm *Bitmap) any {
			buf := make([]byte, bm.LenInBytes()*2)
			return bm.CloneToBuf(buf).ToArray()
		}},
		{"ToBuffer", func(_ *testing.T, bm *Bitmap) any {
			return FromBuffer(bm.Clone().ToBuffer()).ToArray()
		}},
		{"ToBufferWithCopy", func(_ *testing.T, bm *Bitmap) any {
			return FromBuffer(bm.ToBufferWithCopy()).ToArray()
		}},
		{"Split", func(_ *testing.T, bm *Bitmap) any {
			out := []uint64{}
			for _, s := range bm.Split(func(_, _ uint64) uint64 { return 1 }, 1<<20) {
				out = append(out, s.ToArray()...)
			}
			return out
		}},
		{"Intersects", func(_ *testing.T, bm *Bitmap) any {
			return bm.Intersects(bitmapOf(1, 5, 9, 1<<20, 1<<33))
		}},
		{"Masked", func(_ *testing.T, bm *Bitmap) any {
			return bm.Masked(0xFFFFFFFF00000000).ToArray()
		}},
		{"FastOr", func(_ *testing.T, bm *Bitmap) any {
			return FastOr(bm.Clone(), bitmapOf(3, 7, 1<<40)).ToArray()
		}},
		{"FastAnd", func(_ *testing.T, bm *Bitmap) any {
			return FastAnd(bm.Clone(), bm.Clone()).ToArray()
		}},
		{"AndNotPkg", func(_ *testing.T, bm *Bitmap) any {
			return AndNot(bm, bitmapOf(2, 4, 6, 1<<40)).ToArray()
		}},
		{"AndPkg", func(_ *testing.T, bm *Bitmap) any {
			return And(bm, bm.Clone()).ToArray()
		}},
		{"OrPkg", func(_ *testing.T, bm *Bitmap) any {
			return Or(bm, bitmapOf(11, 13, 1<<40)).ToArray()
		}},
		{"SetThenRead", func(_ *testing.T, bm *Bitmap) any {
			c := bm.Clone()
			c.Set(1 << 41)
			c.Set(1)
			return []any{c.GetCardinality(), c.ToArray()}
		}},
		{"RemoveThenRead", func(_ *testing.T, bm *Bitmap) any {
			c := bm.Clone()
			for _, v := range bm.ToArray()[:min(16, bm.GetCardinality())] {
				c.Remove(v)
			}
			return []any{c.GetCardinality(), c.ToArray()}
		}},
		{"RemoveRangeThenRead", func(_ *testing.T, bm *Bitmap) any {
			c := bm.Clone()
			c.RemoveRange(0, 2048)
			return []any{c.GetCardinality(), c.ToArray()}
		}},
		{"CleanupThenRead", func(_ *testing.T, bm *Bitmap) any {
			c := bm.Clone()
			c.Cleanup()
			return []any{c.GetCardinality(), c.ToArray()}
		}},
		{"FillUpThenRead", func(_ *testing.T, bm *Bitmap) any {
			c := bm.Clone()
			c.FillUp(c.Maximum() + 5000)
			return []any{c.GetCardinality(), c.ToArray()}
		}},
		{"ConvertToBitmapContainers", func(_ *testing.T, bm *Bitmap) any {
			c := bm.Clone()
			c.ConvertToBitmapContainers()
			return []any{c.GetCardinality(), c.ToArray()}
		}},
	}
}

// runObservationParity asserts that every observation reads the same value from
// a bitmap holding deferred headers as from one holding exact headers. The
// clones inside the mutating cases are taken from each side separately, so a
// mutation that mis-reads a deferred header shows up as a diverging result
// rather than being hidden by a shared reference.
func runObservationParity(t *testing.T, lazy *Bitmap) {
	t.Helper()
	require.Greater(t, countLazyContainers(lazy), 0,
		"fixture holds no deferred headers, so this asserts nothing")
	exact := settled(lazy)

	for _, obs := range observations() {
		t.Run(obs.name, func(t *testing.T) {
			// re-derive both sides per case: several observations mutate a clone
			lz := settledCopyOf(lazy, false)
			ex := settledCopyOf(lazy, true)
			require.Equal(t, obs.read(t, ex), obs.read(t, lz), obs.name)
		})
	}
	require.Equal(t, exact.GetCardinality(), lazy.GetCardinality())
}

// settledCopyOf copies bm's bits, optionally writing out the cardinality
// headers. Both copies hold identical bits either way.
func settledCopyOf(bm *Bitmap, settle bool) *Bitmap {
	cp := make([]byte, bm.LenInBytes())
	copy(cp, toByteSlice(bm.data))
	out := FromBuffer(cp)
	if settle {
		out.reconcileCardinality()
	}
	return out
}

func TestLazyCardinality_ObservationParity(t *testing.T) {
	cases := []struct {
		name  string
		build func() *Bitmap
	}{
		{"single dense container", func() *Bitmap {
			a := denseContainer(0, 30000, 11)
			b := denseContainer(0, 30000, 12)
			a.ConvertToBitmapContainers()
			b.ConvertToBitmapContainers()
			return a.OrConc(b, 1)
		}},
		{"multi container or", func() *Bitmap {
			a, b := NewBitmap(), NewBitmap()
			rnd := rand.New(rand.NewSource(21))
			for i := 0; i < 200000; i++ {
				a.Set(uint64(rnd.Intn(1 << 20)))
				b.Set(uint64(rnd.Intn(1 << 20)))
			}
			a.ConvertToBitmapContainers()
			b.ConvertToBitmapContainers()
			return a.OrConc(b, 4)
		}},
		{"multi container and", func() *Bitmap {
			a, b := NewBitmap(), NewBitmap()
			rnd := rand.New(rand.NewSource(22))
			for i := 0; i < 300000; i++ {
				a.Set(uint64(rnd.Intn(1 << 20)))
				b.Set(uint64(rnd.Intn(1 << 20)))
			}
			a.ConvertToBitmapContainers()
			b.ConvertToBitmapContainers()
			return a.AndConc(b, 4)
		}},
		{"sparse keys far apart", func() *Bitmap {
			a, b := NewBitmap(), NewBitmap()
			for _, k := range []uint64{0, 1 << 16, 1 << 32, 1 << 48} {
				for i := 0; i < 9000; i++ {
					a.Set(k | uint64(2*i))
					b.Set(k | uint64(i))
				}
			}
			a.ConvertToBitmapContainers()
			b.ConvertToBitmapContainers()
			return a.OrConc(b, 2)
		}},
		{"full and partial containers mixed", func() *Bitmap {
			a := fullContainer(0)
			a.Or(denseContainer(1<<16, 20000, 31))
			b := denseContainer(0, 25000, 32)
			b.Or(denseContainer(1<<16, 25000, 33))
			a.ConvertToBitmapContainers()
			b.ConvertToBitmapContainers()
			return a.OrConc(b, 1)
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { runObservationParity(t, c.build()) })
	}
}

// TestLazyCardinality_EdgeShapes covers the container shapes that make the two
// accumulators answer at their boundaries: no words set, every word set, one
// bit, and the highest and lowest value in a container.
func TestLazyCardinality_EdgeShapes(t *testing.T) {
	cases := []struct {
		name string
		a, b []uint64
	}{
		{"both empty", nil, nil},
		{"a empty", nil, []uint64{1, 2, 3}},
		{"b empty", []uint64{1, 2, 3}, nil},
		{"single element each, equal", []uint64{7}, []uint64{7}},
		{"single element each, disjoint", []uint64{7}, []uint64{8}},
		{"container boundaries", []uint64{0, 65535, 65536, 131071}, []uint64{65535, 65536}},
		{"top of key space", []uint64{math.MaxUint64 &^ 0xFFFF, math.MaxUint64}, []uint64{math.MaxUint64}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, dense := range []bool{false, true} {
				a, b := bitmapOf(c.a...), bitmapOf(c.b...)
				if dense {
					a.ConvertToBitmapContainers()
					b.ConvertToBitmapContainers()
				}
				gotOr := a.Clone().OrConc(b, 1)
				gotAnd := a.Clone().AndConc(b, 1)
				gotAndNot := a.Clone().AndNotConc(b, 1)

				require.Equal(t, Or(a, b).ToArray(), gotOr.ToArray(), "or dense=%v", dense)
				require.Equal(t, And(a, b).ToArray(), gotAnd.ToArray(), "and dense=%v", dense)
				require.Equal(t, AndNot(a, b).ToArray(), gotAndNot.ToArray(), "andnot dense=%v", dense)
				require.Equal(t, len(Or(a, b).ToArray()), gotOr.GetCardinality())
				require.Equal(t, len(And(a, b).ToArray()), gotAnd.GetCardinality())
			}
		})
	}
}

// TestLazyCardinality_AllOnesAndAllZeroWords drives the two accumulators with
// whole words that are entirely set or entirely clear, the inputs where a
// popcount-free empty/full answer either holds or does not.
func TestLazyCardinality_AllOnesAndAllZeroWords(t *testing.T) {
	// one all-ones 64-bit word at the start of an otherwise empty container
	word := NewBitmap()
	for i := 0; i < 64; i++ {
		word.Set(uint64(i))
	}
	word.ConvertToBitmapContainers()

	full := fullContainer(0)
	empty := NewBitmap()
	empty.Set(1 << 20)
	empty.RemoveRange(1<<20, 1<<20+1)

	for _, tc := range []struct {
		name string
		a, b *Bitmap
	}{
		{"word vs full", word, full},
		{"full vs word", full, word},
		{"full vs full", full, full},
		{"word vs empty", word, empty},
		{"full vs empty", full, empty},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotOr := tc.a.Clone().OrConc(tc.b, 1)
			gotAnd := tc.a.Clone().AndConc(tc.b, 1)
			require.Equal(t, Or(tc.a, tc.b).GetCardinality(), gotOr.GetCardinality())
			require.Equal(t, And(tc.a, tc.b).GetCardinality(), gotAnd.GetCardinality())
			require.Equal(t, Or(tc.a, tc.b).ToArray(), gotOr.ToArray())
			require.Equal(t, And(tc.a, tc.b).ToArray(), gotAnd.ToArray())
		})
	}
}

// TestLazyCardinality_CascadeAgainstOracles folds a bit-sliced plane cascade —
// the shape a numeric range filter produces — with the in-place operators, and
// checks it against roaring64 and against the allocating operators, which never
// defer.
func TestLazyCardinality_CascadeAgainstOracles(t *testing.T) {
	const (
		planes = 12
		docs   = 1 << 18
	)

	for _, conc := range []int{1, 4, 8} {
		for seed := int64(1); seed <= 4; seed++ {
			conc, seed := conc, seed
			t.Run(fmt.Sprintf("conc=%d/seed=%d", conc, seed), func(t *testing.T) {
				rnd := rand.New(rand.NewSource(seed*100 + int64(conc)))
				sroarPlanes := make([]*Bitmap, planes)
				roarPlanes := make([]*roaring64.Bitmap, planes)
				for p := 0; p < planes; p++ {
					sroarPlanes[p] = NewBitmap()
					roarPlanes[p] = roaring64.New()
					// plane 0 is the universe; deeper planes are subsets of it,
					// matching how a range cascade is built
					density := 0.9
					if p > 0 {
						density = 0.2 + 0.6*rnd.Float64()
					}
					for d := uint64(0); d < docs; d++ {
						if p > 0 && !sroarPlanes[0].Contains(d) {
							continue
						}
						if rnd.Float64() < density {
							sroarPlanes[p].Set(d)
							roarPlanes[p].Add(d)
						}
					}
					sroarPlanes[p].ConvertToBitmapContainers()
				}

				got := sroarPlanes[0].Clone()
				want := roarPlanes[0].Clone()
				exact := sroarPlanes[0].Clone()
				for p := 1; p < planes; p++ {
					if rnd.Intn(2) == 0 {
						got.AndConc(sroarPlanes[p], conc)
						want.And(roarPlanes[p])
						exact = And(exact, sroarPlanes[p])
					} else {
						got.OrConc(sroarPlanes[p], conc)
						want.Or(roarPlanes[p])
						exact = Or(exact, sroarPlanes[p])
					}
				}

				require.Equal(t, int(want.GetCardinality()), got.GetCardinality())
				require.Equal(t, want.ToArray(), got.ToArray())
				require.Equal(t, exact.GetCardinality(), got.GetCardinality())
				require.Equal(t, exact.ToArray(), got.ToArray())
			})
		}
	}
}

// TestLazyCardinality_AgainstOldFamily checks the in-place operators against
// sroar's own pre-existing implementations, which compute cardinality eagerly.
func TestLazyCardinality_AgainstOldFamily(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	for iter := 0; iter < 40; iter++ {
		a, b := NewBitmap(), NewBitmap()
		span := 1 << uint(12+rnd.Intn(9))
		for i := 0; i < span/2; i++ {
			a.Set(uint64(rnd.Intn(span)))
			b.Set(uint64(rnd.Intn(span)))
		}
		if rnd.Intn(2) == 0 {
			a.ConvertToBitmapContainers()
		}
		if rnd.Intn(2) == 0 {
			b.ConvertToBitmapContainers()
		}

		wantOr := a.Clone()
		wantOr.OrOld(b)
		gotOr := a.Clone().OrConc(b, 1+rnd.Intn(8))
		require.Equal(t, wantOr.ToArray(), gotOr.ToArray(), "iter %d or", iter)
		require.Equal(t, wantOr.GetCardinality(), gotOr.GetCardinality(), "iter %d or card", iter)

		wantAnd := a.Clone()
		wantAnd.AndOld(b)
		gotAnd := a.Clone().AndConc(b, 1+rnd.Intn(8))
		require.Equal(t, wantAnd.ToArray(), gotAnd.ToArray(), "iter %d and", iter)
		require.Equal(t, wantAnd.GetCardinality(), gotAnd.GetCardinality(), "iter %d and card", iter)

		wantAndNot := a.Clone()
		wantAndNot.AndNotOld(b)
		gotAndNot := a.Clone().AndNotConc(b, 1+rnd.Intn(8))
		require.Equal(t, wantAndNot.ToArray(), gotAndNot.ToArray(), "iter %d andnot", iter)
	}
}

// TestLazyCardinality_ChainedMergesStayCorrect checks that a deferred header
// surviving into the next merge — the whole point of the change — still yields
// the right answer whichever container types the operands take.
func TestLazyCardinality_ChainedMergesStayCorrect(t *testing.T) {
	rnd := rand.New(rand.NewSource(99))
	for iter := 0; iter < 30; iter++ {
		acc := NewBitmap()
		for i := 0; i < 20000; i++ {
			acc.Set(uint64(rnd.Intn(1 << 18)))
		}
		acc.ConvertToBitmapContainers()
		ref := acc.Clone()

		for step := 0; step < 10; step++ {
			other := NewBitmap()
			for i := 0; i < 200+rnd.Intn(30000); i++ {
				other.Set(uint64(rnd.Intn(1 << 18)))
			}
			// leave roughly half the operands as array containers so the
			// mixed array/bitmap paths see a deferred destination
			if rnd.Intn(2) == 0 {
				other.ConvertToBitmapContainers()
			}
			switch rnd.Intn(3) {
			case 0:
				acc.OrConc(other, 1+rnd.Intn(4))
				ref = Or(ref, other)
			case 1:
				acc.AndConc(other, 1+rnd.Intn(4))
				ref = And(ref, other)
			default:
				acc.AndNotConc(other, 1+rnd.Intn(4))
				ref = AndNot(ref, other)
			}
			require.Equal(t, ref.ToArray(), acc.ToArray(), "iter %d step %d", iter, step)
			require.Equal(t, ref.GetCardinality(), acc.GetCardinality(), "iter %d step %d", iter, step)
		}
	}
}

// TestLazyCardinality_ConcurrentObserversAreReadOnly proves the observation
// path never writes: a bitmap holding deferred headers is read from many
// goroutines at once, and the race detector has to stay quiet.
func TestLazyCardinality_ConcurrentObserversAreReadOnly(t *testing.T) {
	a := denseContainer(0, 30000, 41)
	b := denseContainer(0, 30000, 42)
	a.ConvertToBitmapContainers()
	b.ConvertToBitmapContainers()
	lazy := a.OrConc(b, 1)
	require.Greater(t, countLazyContainers(lazy), 0)

	want := settled(lazy).GetCardinality()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.Equal(t, want, lazy.GetCardinality())
			require.False(t, lazy.IsEmpty())
			require.Equal(t, want, len(lazy.ToArray()))
			require.Equal(t, want, lazy.Clone().GetCardinality())
			require.Equal(t, want, FromBuffer(lazy.ToBufferWithCopy()).GetCardinality())
			it := lazy.NewIterator()
			n := 0
			for v := it.Next(); v != 0; v = it.Next() {
				n++
			}
			require.LessOrEqual(t, n, want)
		}()
	}
	wg.Wait()

	// still deferred: no observer wrote a header back
	require.Greater(t, countLazyContainers(lazy), 0)
}

// TestLazyCardinality_SerializedHeadersAreExact pins that a deferred header
// never reaches the wire format, where a reader outside this package would
// take it at face value.
func TestLazyCardinality_SerializedHeadersAreExact(t *testing.T) {
	build := func() *Bitmap {
		a := denseContainer(0, 30000, 51)
		b := denseContainer(0, 30000, 52)
		a.ConvertToBitmapContainers()
		b.ConvertToBitmapContainers()
		return a.OrConc(b, 1)
	}

	t.Run("ToBuffer", func(t *testing.T) {
		bm := build()
		require.Greater(t, countLazyContainers(bm), 0)
		out := FromBuffer(bm.ToBuffer())
		require.Equal(t, 0, countLazyContainers(out))
		require.Equal(t, 0, countLazyContainers(bm))
	})

	t.Run("ToBufferWithCopy leaves the source untouched", func(t *testing.T) {
		bm := build()
		before := countLazyContainers(bm)
		out := FromBuffer(bm.ToBufferWithCopy())
		require.Equal(t, 0, countLazyContainers(out))
		require.Equal(t, before, countLazyContainers(bm))
	})

	t.Run("Clone leaves the source untouched", func(t *testing.T) {
		bm := build()
		before := countLazyContainers(bm)
		out := bm.Clone()
		require.Equal(t, 0, countLazyContainers(out))
		require.Equal(t, before, countLazyContainers(bm))
		require.Equal(t, settled(bm).ToArray(), out.ToArray())
	})
}
