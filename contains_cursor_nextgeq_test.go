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
}

// Leapfrog successor scan: the sparse-filter iteration pattern NextGeq exists
// for (jump, land, jump), vs re-probing candidates one by one with Contains.
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
