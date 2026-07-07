package sroar

import (
	"math/rand"
	"sync"
	"testing"
)

// The fixtures are deterministic (fixed seeds), expensive to build (millions of
// inserts), and only ever probed — so they are constructed at most once per test
// binary and shared read-only across tests and benchmarks.
var (
	denseOnce   sync.Once
	denseShared *Bitmap
	mixedOnce   sync.Once
	mixedShared *Bitmap
)

// denseBitmap spreads 2M values over a 200M range: ~3050 array containers of
// ~650 values each. Exercises the array-container paths.
func denseBitmap() *Bitmap {
	denseOnce.Do(func() {
		b := NewBitmap()
		rng := rand.New(rand.NewSource(7))
		max := int64(200_000_000)
		for i := 0; i < 2_000_000; i++ {
			b.Set(uint64(rng.Int63n(max)))
		}
		denseShared = b
	})
	return denseShared
}

// mixedBitmap combines fully-populated bitmap containers, mid-density array
// containers, empty key ranges, and a sparse tail — so probes cross every
// container type and every miss shape.
func mixedBitmap() *Bitmap {
	mixedOnce.Do(func() {
		mixedShared = buildMixedBitmap()
	})
	return mixedShared
}

func buildMixedBitmap() *Bitmap {
	b := NewBitmap()
	// bitmap containers: every value in [0, 4<<16)
	for x := uint64(0); x < 4<<16; x++ {
		b.Set(x)
	}
	// gap: no containers for keys in [4<<16, 8<<16)
	// array containers: ~700/container over [8<<16, 12<<16)
	rng := rand.New(rand.NewSource(21))
	for i := 0; i < 2800; i++ {
		b.Set(uint64(8<<16) + uint64(rng.Int63n(4<<16)))
	}
	// dense-but-not-full bitmap containers: every even value in [16<<16, 18<<16)
	for x := uint64(16 << 16); x < 18<<16; x += 2 {
		b.Set(x)
	}
	// sparse tail: single-value containers far out
	for i := uint64(0); i < 20; i++ {
		b.Set(uint64(32<<16) + i*3*65536)
	}
	return b
}

// The cursor must match Bitmap.Contains for any access order, on both array and
// bitmap containers, hits and misses, present and absent keys.
func TestContainsCursorEquivalence(t *testing.T) {
	fixtures := []struct {
		name string
		bm   *Bitmap
		max  uint64
	}{
		{"dense_arrays", denseBitmap(), 200_000_000},
		{"mixed_containers", mixedBitmap(), 90 << 16},
	}
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(99))
			cur := fx.bm.NewContainsCursor()
			for i := 0; i < 2_000_000; i++ { // random: forward gallops + backward fallbacks
				x := uint64(rng.Int63n(int64(fx.max)))
				if cur.Contains(x) != fx.bm.Contains(x) {
					t.Fatalf("random mismatch at %d", x)
				}
			}
			cur2 := fx.bm.NewContainsCursor()
			for x := uint64(0); x < fx.max; x += uint64(rng.Intn(97) + 1) { // monotonic
				if cur2.Contains(x) != fx.bm.Contains(x) {
					t.Fatalf("monotonic mismatch at %d", x)
				}
			}
		})
	}
}

func TestContainsCursorEdgeCases(t *testing.T) {
	t.Run("nil_cursor_and_nil_bitmap", func(t *testing.T) {
		var nilCur *ContainsCursor
		if nilCur.Contains(42) {
			t.Fatal("nil cursor must report false")
		}
		var nilBm *Bitmap
		if nilBm.NewContainsCursor().Contains(42) {
			t.Fatal("cursor over nil bitmap must report false")
		}
	})
	t.Run("zero_value_cursor", func(t *testing.T) {
		var cur ContainsCursor // embedded-by-value, never Reset
		if cur.Contains(0) || cur.Contains(42) {
			t.Fatal("zero-value cursor must report false")
		}
	})
	t.Run("empty_bitmap", func(t *testing.T) {
		cur := NewBitmap().NewContainsCursor()
		for _, x := range []uint64{0, 1, 65535, 65536, 1 << 40} {
			if cur.Contains(x) {
				t.Fatalf("empty bitmap: %d must be false", x)
			}
		}
	})
	t.Run("single_container", func(t *testing.T) {
		b := NewBitmap()
		b.Set(100)
		b.Set(200)
		cur := b.NewContainsCursor()
		checks := []struct {
			x    uint64
			want bool
		}{{0, false}, {100, true}, {150, false}, {200, true}, {65535, false}, {1 << 30, false}}
		for _, c := range checks {
			if got := cur.Contains(c.x); got != c.want {
				t.Fatalf("Contains(%d)=%v want %v", c.x, got, c.want)
			}
		}
	})
	t.Run("beyond_max_then_backward", func(t *testing.T) {
		b := denseBitmap()
		cur := b.NewContainsCursor()
		if cur.Contains(1 << 50) { // far past the last container
			t.Fatal("beyond-max must be false")
		}
		// backward probes after exhausting the key space must still be exact
		rng := rand.New(rand.NewSource(5))
		for i := 0; i < 100_000; i++ {
			x := uint64(rng.Int63n(200_000_000))
			if cur.Contains(x) != b.Contains(x) {
				t.Fatalf("backward-after-end mismatch at %d", x)
			}
		}
	})
	t.Run("reset_rebind", func(t *testing.T) {
		a, b := denseBitmap(), mixedBitmap()
		var cur ContainsCursor
		cur.Reset(a)
		rng := rand.New(rand.NewSource(11))
		for i := 0; i < 100_000; i++ {
			x := uint64(rng.Int63n(200_000_000))
			if cur.Contains(x) != a.Contains(x) {
				t.Fatalf("bound to a: mismatch at %d", x)
			}
		}
		cur.Reset(b) // rebind mid-life: no state may leak
		for i := 0; i < 100_000; i++ {
			x := uint64(rng.Int63n(90 << 16))
			if cur.Contains(x) != b.Contains(x) {
				t.Fatalf("rebound to b: mismatch at %d", x)
			}
		}
		cur.Reset(nil)
		if cur.Contains(1) {
			t.Fatal("rebound to nil: must report false")
		}
	})
}

// Contains vs ContainsCursor across three probe patterns: small forward gaps
// (probes mostly repeat a container), large forward gaps (every probe a new
// container, where the forward gallop beats the from-scratch search), and fully
// random (no locality — the cursor's worst case).
func BenchmarkContainsCursor(b *testing.B) {
	bm := denseBitmap()
	max := uint64(200_000_000)

	ascProbes := func(avgStep int) []uint64 {
		rng := rand.New(rand.NewSource(7))
		var probes []uint64
		for x := uint64(0); x < max; x += uint64(rng.Intn(avgStep) + 1) {
			probes = append(probes, x)
		}
		return probes
	}
	runAsc := func(b *testing.B, probes []uint64, useCursor bool) {
		b.ReportAllocs()
		// increment-and-wrap rather than i%n: a hardware integer division in the
		// timed loop would compress the measured Contains-vs-Cursor ratio.
		n := len(probes)
		var s int
		j := -1
		cur := bm.NewContainsCursor()
		for i := 0; i < b.N; i++ {
			j++
			if j == n {
				j = 0
				cur.Reset(bm) // wrapped: keep probes non-decreasing within a pass
			}
			hit := false
			if useCursor {
				hit = cur.Contains(probes[j])
			} else {
				hit = bm.Contains(probes[j])
			}
			if hit {
				s++
			}
		}
		sink = s
	}

	for _, rg := range []struct {
		name    string
		avgStep int
	}{{"smallgap", 50}, {"biggap", 400_000}} {
		probes := ascProbes(rg.avgStep)
		b.Run(rg.name+"/Contains", func(b *testing.B) { runAsc(b, probes, false) })
		b.Run(rg.name+"/Cursor", func(b *testing.B) { runAsc(b, probes, true) })
	}

	rng := rand.New(rand.NewSource(11))
	randp := make([]uint64, 1<<20)
	for i := range randp {
		randp[i] = uint64(rng.Int63n(int64(max)))
	}
	m := len(randp)
	b.Run("random/Contains", func(b *testing.B) {
		b.ReportAllocs()
		var s int
		for i := 0; i < b.N; i++ {
			if bm.Contains(randp[i%m]) {
				s++
			}
		}
		sink = s
	})
	b.Run("random/Cursor", func(b *testing.B) {
		b.ReportAllocs()
		cur := bm.NewContainsCursor()
		var s int
		for i := 0; i < b.N; i++ {
			if cur.Contains(randp[i%m]) {
				s++
			}
		}
		sink = s
	})
}

var sink int

// The reject-walk probe shapes: misses landing in the gap at the cursor and
// re-probes of the value the cursor sits on. Both are arrayHas early returns —
// one compare, no arrPos store — and regress first if that fast path is lost.
func BenchmarkContainsCursorGapShapes(b *testing.B) {
	bm := NewBitmap()
	for x := uint64(0); x < 8<<16; x += 17 { // ~3855/container: array containers
		bm.Set(x)
	}
	max := uint64(8 << 16)

	b.Run("gap_reject", func(b *testing.B) {
		cur := bm.NewContainsCursor()
		var s int
		y := uint64(1)
		for i := 0; i < b.N; i++ {
			if cur.Contains(y) { // 17k+1: always a miss in the gap ahead
				s++
			}
			y += 17
			if y >= max {
				y = 1
			}
		}
		sink = s
	})
	b.Run("at_cursor_hit", func(b *testing.B) {
		cur := bm.NewContainsCursor()
		var s int
		y := uint64(0)
		for i := 0; i < b.N; i += 2 {
			if cur.Contains(y) { // advance to y (hit)
				s++
			}
			if cur.Contains(y) { // re-probe the cursor position (hit)
				s++
			}
			y += 17
			if y >= max {
				y = 0
			}
		}
		sink = s
	})
}
