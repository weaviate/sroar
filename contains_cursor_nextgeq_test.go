package sroar

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
)

var (
	denseValsOnce sync.Once
	denseVals     []uint64
	mixedValsOnce sync.Once
	mixedVals     []uint64
)

func denseValues() []uint64 {
	denseValsOnce.Do(func() { denseVals = denseBitmap().ToArray() })
	return denseVals
}

func mixedValues() []uint64 {
	mixedValsOnce.Do(func() { mixedVals = mixedBitmap().ToArray() })
	return mixedVals
}

func refNextGeq(vals []uint64, x uint64) (uint64, bool) {
	i := sort.Search(len(vals), func(i int) bool { return vals[i] >= x })
	if i < len(vals) {
		return vals[i], true
	}
	return 0, false
}

// NextGeq must return the exact successor for any probe, in any order, on both
// array and bitmap containers.
func TestNextGeqEquivalence(t *testing.T) {
	fixtures := []struct {
		name string
		bm   *Bitmap
		vals []uint64
		max  uint64
	}{
		{"dense_arrays", denseBitmap(), denseValues(), 200_000_000},
		{"mixed_containers", mixedBitmap(), mixedValues(), 90 << 16},
	}
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(31))

			// leapfrog: ascending probes jumping from each successor
			cur := fx.bm.NewContainsCursor()
			x := uint64(0)
			for x < fx.max {
				got, gotOK := cur.NextGeq(x)
				want, wantOK := refNextGeq(fx.vals, x)
				if gotOK != wantOK || got != want {
					t.Fatalf("leapfrog NextGeq(%d) = (%d,%v), want (%d,%v)", x, got, gotOK, want, wantOK)
				}
				if !gotOK {
					break
				}
				if !cur.Contains(got) { // returned value must be set, cursor stays consistent
					t.Fatalf("Contains(NextGeq(%d)=%d) = false", x, got)
				}
				x = got + 1 + uint64(rng.Intn(100_000))
			}

			// random probes on a shared cursor (forward and backward jumps)
			cur2 := fx.bm.NewContainsCursor()
			for i := 0; i < 300_000; i++ {
				x := uint64(rng.Int63n(int64(fx.max)))
				got, gotOK := cur2.NextGeq(x)
				want, wantOK := refNextGeq(fx.vals, x)
				if gotOK != wantOK || got != want {
					t.Fatalf("random NextGeq(%d) = (%d,%v), want (%d,%v)", x, got, gotOK, want, wantOK)
				}
			}

			// interleaved Contains + NextGeq, ascending: both must stay exact
			cur3 := fx.bm.NewContainsCursor()
			x = 0
			for i := 0; i < 200_000 && x < fx.max; i++ {
				if i%3 == 0 {
					got, gotOK := cur3.NextGeq(x)
					want, wantOK := refNextGeq(fx.vals, x)
					if gotOK != wantOK || got != want {
						t.Fatalf("interleaved NextGeq(%d) = (%d,%v), want (%d,%v)", x, got, gotOK, want, wantOK)
					}
				} else {
					want, _ := refNextGeq(fx.vals, x)
					if got := cur3.Contains(x); got != (want == x) {
						t.Fatalf("interleaved Contains(%d) = %v, want %v", x, got, want == x)
					}
				}
				x += uint64(rng.Intn(200) + 1)
			}

			// exact boundaries
			first, last := fx.vals[0], fx.vals[len(fx.vals)-1]
			cur4 := fx.bm.NewContainsCursor()
			if v, ok := cur4.NextGeq(0); !ok || v != first {
				t.Fatalf("NextGeq(0) = (%d,%v), want (%d,true)", v, ok, first)
			}
			if v, ok := cur4.NextGeq(last); !ok || v != last {
				t.Fatalf("NextGeq(last) = (%d,%v), want (%d,true)", v, ok, last)
			}
			if _, ok := cur4.NextGeq(last + 1); ok {
				t.Fatal("NextGeq(last+1) must not find a successor")
			}
		})
	}
}

func TestNextGeqEdgeCases(t *testing.T) {
	t.Run("nil_and_empty", func(t *testing.T) {
		var nilCur *ContainsCursor
		if _, ok := nilCur.NextGeq(0); ok {
			t.Fatal("nil cursor")
		}
		if _, ok := NewBitmap().NewContainsCursor().NextGeq(0); ok {
			t.Fatal("empty bitmap")
		}
	})
	t.Run("skips_emptied_container", func(t *testing.T) {
		b := NewBitmap()
		b.Set(5)
		b.Remove(5) // key-0 container remains, empty
		b.Set(1<<16 + 3)
		cur := b.NewContainsCursor()
		if v, ok := cur.NextGeq(0); !ok || v != 1<<16+3 {
			t.Fatalf("NextGeq(0) = (%d,%v), want (%d,true)", v, ok, 1<<16+3)
		}
	})
	t.Run("crosses_container_gap", func(t *testing.T) {
		b := NewBitmap()
		b.Set(100)
		b.Set(7<<16 + 9) // keys 0 and 7, nothing between
		cur := b.NewContainsCursor()
		if v, ok := cur.NextGeq(101); !ok || v != 7<<16+9 {
			t.Fatalf("NextGeq(101) = (%d,%v)", v, ok)
		}
	})
	t.Run("within_bitmap_container", func(t *testing.T) {
		b := NewBitmap()
		for x := uint64(0); x < 65536; x += 7 {
			b.Set(x) // dense: bitmap container
		}
		vals := b.ToArray()
		cur := b.NewContainsCursor()
		rng := rand.New(rand.NewSource(3))
		for i := 0; i < 100_000; i++ {
			x := uint64(rng.Intn(65600))
			got, gotOK := cur.NextGeq(x)
			want, wantOK := refNextGeq(vals, x)
			if gotOK != wantOK || got != want {
				t.Fatalf("NextGeq(%d) = (%d,%v), want (%d,%v)", x, got, gotOK, want, wantOK)
			}
		}
	})
	// A same-key probe past the cached container's last value must fall through
	// to the "cached container exhausted" branch (idx = lastIdx+1) and let a
	// LATER container supply the successor. The random differential reaches the
	// array shape of this often but never the bitmap shape, so assert both.
	t.Run("bitmap_exhausted_then_later_container", func(t *testing.T) {
		b := NewBitmap()
		for x := uint64(0); x < 65536; x += 2 {
			b.Set(x) // 32768 values -> key-0 bitmap container; last set bit 65534
		}
		b.Set(2<<16 + 7)
		cur := b.NewContainsCursor()
		if v, ok := cur.NextGeq(100); !ok || v != 100 { // land in the key-0 bitmap
			t.Fatalf("NextGeq(100) = (%d,%v), want (100,true)", v, ok)
		}
		// same key 0, past the last set bit: bitmap.nextGeq fails, so the walk
		// must advance to the key-2 container's minimum.
		if v, ok := cur.NextGeq(65535); !ok || v != 2<<16+7 {
			t.Fatalf("NextGeq(65535) = (%d,%v), want (%d,true)", v, ok, 2<<16+7)
		}
	})
	t.Run("array_exhausted_then_later_container", func(t *testing.T) {
		b := NewBitmap()
		b.Set(10)
		b.Set(20) // key-0 array container
		b.Set(2<<16 + 7)
		cur := b.NewContainsCursor()
		if v, ok := cur.NextGeq(10); !ok || v != 10 { // land in the key-0 array
			t.Fatalf("NextGeq(10) = (%d,%v), want (10,true)", v, ok)
		}
		if v, ok := cur.NextGeq(21); !ok || v != 2<<16+7 { // past the array's max
			t.Fatalf("NextGeq(21) = (%d,%v), want (%d,true)", v, ok, 2<<16+7)
		}
	})
}

// Ascending small-gap jumps over denseBitmap (all array containers): jumps of
// ~1-2000 against ~650-value/container arrays keep almost every probe in the
// same container, so this measures the array same-container fast path
// (arrayGeqPos). It does NOT exercise bitmap.nextGeq or the cross-container
// walk — see BenchmarkNextGeqBitmap and BenchmarkNextGeqCrossContainer for those.
func BenchmarkNextGeqLeapfrog(b *testing.B) {
	bm := denseBitmap()
	max := uint64(200_000_000)
	rng := rand.New(rand.NewSource(9))
	// precompute jump offsets to keep RNG out of the timed loop
	gaps := make([]uint64, 1<<16)
	for i := range gaps {
		gaps[i] = uint64(rng.Intn(2000) + 1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	cur := bm.NewContainsCursor()
	x := uint64(0)
	var s int
	for i := 0; i < b.N; i++ {
		v, ok := cur.NextGeq(x)
		if !ok {
			cur.Reset(bm)
			x = 0
			continue
		}
		s++
		x = v + gaps[i&(1<<16-1)]
		if x >= max {
			cur.Reset(bm)
			x = 0
		}
	}
	sink = s
}

var (
	bitmapContainersOnce   sync.Once
	bitmapContainersShared *Bitmap
)

// bitmapContainerFixture builds a bitmap whose every container is a bitmap
// container (cardinality > 4096), sparse enough (every 5th value) that
// bitmap.nextGeq scans real zero-runs. The denseBitmap/mixedBitmap array
// containers never enter bitmap.nextGeq, so this fixture is needed to bench it.
func bitmapContainerFixture() *Bitmap {
	bitmapContainersOnce.Do(func() {
		b := NewBitmap()
		for k := uint64(0); k < 16; k++ {
			base := k << 16
			for v := uint64(0); v < 1<<16; v += 5 { // ~13108/key -> bitmap container
				b.Set(base | v)
			}
		}
		bitmapContainersShared = b
	})
	return bitmapContainersShared
}

// BenchmarkNextGeqBitmap is the bitmap-container analog of
// BenchmarkNextGeqLeapfrog: small ascending jumps that mostly stay inside one
// bitmap container, so the timed path is bitmap.nextGeq (the word-masked scan
// this PR adds) rather than the array fast path.
func BenchmarkNextGeqBitmap(b *testing.B) {
	bm := bitmapContainerFixture()
	max := uint64(16 << 16)
	rng := rand.New(rand.NewSource(9))
	gaps := make([]uint64, 1<<16)
	for i := range gaps {
		gaps[i] = uint64(rng.Intn(2000) + 1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	cur := bm.NewContainsCursor()
	x := uint64(0)
	var s int
	for i := 0; i < b.N; i++ {
		v, ok := cur.NextGeq(x)
		if !ok {
			cur.Reset(bm)
			x = 0
			continue
		}
		s++
		x = v + gaps[i&(1<<16-1)]
		if x >= max {
			cur.Reset(bm)
			x = 0
		}
	}
	sink = s
}

// BenchmarkNextGeqCrossContainer is a genuine cross-container leapfrog: jumps
// far larger than a container's 65536 span, so every probe changes key and
// exercises the keys searchFrom gallop plus the fresh-container walk (including
// skips over mixedBitmap's empty-key gaps) rather than any same-container path.
func BenchmarkNextGeqCrossContainer(b *testing.B) {
	bm := mixedBitmap()
	max := uint64(90 << 16)
	rng := rand.New(rand.NewSource(9))
	gaps := make([]uint64, 1<<16)
	for i := range gaps {
		gaps[i] = uint64(1<<16) + uint64(rng.Intn(3<<16)) // 1-4 containers forward
	}
	b.ReportAllocs()
	b.ResetTimer()
	cur := bm.NewContainsCursor()
	x := uint64(0)
	var s int
	for i := 0; i < b.N; i++ {
		v, ok := cur.NextGeq(x)
		if !ok {
			cur.Reset(bm)
			x = 0
			continue
		}
		s++
		x = v + gaps[i&(1<<16-1)]
		if x >= max {
			cur.Reset(bm)
			x = 0
		}
	}
	sink = s
}
