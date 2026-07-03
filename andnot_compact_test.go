package sroar

import (
	"math/rand"
	"testing"
)

// AndNot results whose bitmap containers drop below the array threshold must be
// down-converted: semantically identical, materially smaller.
func TestAndNotCompactsSparseResults(t *testing.T) {
	// dense a: every container becomes a bitmap container
	a := NewBitmap()
	for x := uint64(0); x < 1_000_000; x++ {
		a.Set(x)
	}
	// b removes all but ~500 scattered values per container
	b := NewBitmap()
	rng := rand.New(rand.NewSource(3))
	survivors := map[uint64]struct{}{}
	for i := 0; i < 8000; i++ {
		survivors[uint64(rng.Int63n(1_000_000))] = struct{}{}
	}
	for x := uint64(0); x < 1_000_000; x++ {
		if _, ok := survivors[x]; !ok {
			b.Set(x)
		}
	}

	res := AndNot(a, b)

	// semantic equality against per-element reference
	for x := uint64(0); x < 1_000_000; x++ {
		_, want := survivors[x]
		if got := res.Contains(x); got != want {
			t.Fatalf("mismatch at %d: got %v want %v", x, got, want)
		}
	}
	if res.GetCardinality() != len(survivors) {
		t.Fatalf("cardinality %d want %d", res.GetCardinality(), len(survivors))
	}

	// size: ~8k survivors as array containers should be a few bytes/value, far
	// below the ~16 bitmap containers (8KB each) the uncompacted result kept.
	gotBytes := len(res.ToBuffer())
	uncompacted := 16 * 8192
	if gotBytes >= uncompacted/4 {
		t.Fatalf("result not compacted: %d bytes (uncompacted would be ~%d)", gotBytes, uncompacted)
	}
}

// The boundary: results at/above the threshold must stay bitmap containers and
// results just below must convert, both with exact semantics.
func TestAndNotCompactThreshold(t *testing.T) {
	for _, keep := range []int{andNotCompactThreshold - 1, andNotCompactThreshold, 4200} {
		a := NewBitmap()
		for x := uint64(0); x < 65536; x++ {
			a.Set(x) // one full bitmap container
		}
		b := NewBitmap()
		for x := uint64(keep); x < 65536; x++ {
			b.Set(x) // remove all but the first `keep` values
		}
		res := AndNot(a, b)
		if got := res.GetCardinality(); got != keep {
			t.Fatalf("keep=%d: cardinality %d", keep, got)
		}
		for x := uint64(0); x < 65536; x++ {
			want := x < uint64(keep)
			if res.Contains(x) != want {
				t.Fatalf("keep=%d: mismatch at %d", keep, x)
			}
		}
	}
}

// Mixed container shapes through the two-pass sizing: matched array/bitmap pairs
// in all four combinations, unmatched containers, and containers emptied by the
// subtraction — all against a brute-force reference.
func TestAndNotTwoPassMixedShapes(t *testing.T) {
	rng := rand.New(rand.NewSource(17))
	a, b := NewBitmap(), NewBitmap()
	// container 0: a bitmap, b bitmap (partial overlap)
	for x := uint64(0); x < 65536; x++ {
		a.Set(x)
		if x%3 != 0 {
			b.Set(x)
		}
	}
	// container 1: a array, b array
	for i := 0; i < 900; i++ {
		x := uint64(1<<16) + uint64(rng.Int63n(65536))
		a.Set(x)
		if i%2 != 0 {
			b.Set(x)
		}
	}
	// container 2: a array unmatched in b
	for i := 0; i < 500; i++ {
		a.Set(uint64(2<<16) + uint64(rng.Int63n(65536)))
	}
	// container 3: a bitmap fully erased by b
	for x := uint64(3 << 16); x < 4<<16; x++ {
		a.Set(x)
		b.Set(x)
	}
	// container 4: b-only (must not appear)
	for i := 0; i < 100; i++ {
		b.Set(uint64(4<<16) + uint64(rng.Int63n(65536)))
	}

	want := NewBitmap()
	for _, x := range a.ToArray() {
		if !b.Contains(x) {
			want.Set(x)
		}
	}

	res := AndNot(a, b)
	if res.GetCardinality() != want.GetCardinality() {
		t.Fatalf("cardinality %d want %d", res.GetCardinality(), want.GetCardinality())
	}
	for _, x := range want.ToArray() {
		if !res.Contains(x) {
			t.Fatalf("missing %d", x)
		}
	}
	for x := uint64(0); x < 5<<16; x += 7 {
		if res.Contains(x) != want.Contains(x) {
			t.Fatalf("mismatch at %d", x)
		}
	}
}

// Every container offset in an AndNot result must stay a multiple of 4 uint16s:
// uint16To64SliceUnsafe builds *uint64 views over container payloads, so a
// misaligned compacted container would be UB (and a SIGBUS on strict-alignment
// targets) for every operation that later touches res.
func TestAndNotResultAlignment(t *testing.T) {
	checkAligned := func(t *testing.T, res *Bitmap) {
		t.Helper()
		for i := 0; i < res.keys.numKeys(); i++ {
			off := res.keys.val(i)
			if off%4 != 0 {
				t.Fatalf("container %d (key %d) at misaligned offset %d", i, res.keys.key(i), off)
			}
			c := res.getContainer(off)
			if len(c)%4 != 0 {
				t.Fatalf("container %d has unpadded size %d", i, len(c))
			}
		}
	}

	// the shape that put a compacted container ahead of a bitmap container
	a, b := NewBitmap(), NewBitmap()
	for x := uint64(0); x < 131072; x++ {
		a.Set(x)
	}
	for x := uint64(100); x < 65536; x++ {
		b.Set(x)
	}
	res := AndNot(a, b)
	checkAligned(t, res)
	// exercise the unsafe uint64 views over the result
	if got := res.Maximum(); got != 131071 {
		t.Fatalf("Maximum on compacted result: %d", got)
	}
	if !res.Intersects(a) {
		t.Fatal("Intersects on compacted result")
	}
	res.AndNot(b) // in-place op over the compacted arena

	// fuzz: random container mixes, all results must stay aligned
	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 30; trial++ {
		a, b := NewBitmap(), NewBitmap()
		for c := 0; c < 6; c++ {
			base := uint64(c) << 16
			an := rng.Intn(60000) + 1
			for i := 0; i < an; i++ {
				a.Set(base + uint64(rng.Intn(65536)))
			}
			if rng.Intn(3) > 0 {
				bn := rng.Intn(65000) + 1
				for i := 0; i < bn; i++ {
					b.Set(base + uint64(rng.Intn(65536)))
				}
			}
		}
		res := AndNot(a, b)
		checkAligned(t, res)
	}
}

// A bitmap container whose cardinality header misstates its real popcount
// (corrupt serialized input) must neither panic nor lose membership. The
// container lives under key 1 because NewBitmap pre-creates a key-0
// container, which would turn the unmatched cases into matched-empty ones.
func TestAndNotCorruptCardinalityHeaders(t *testing.T) {
	const base = uint64(1) << 16
	// newCorrupt returns a bitmap holding [base, base+real) in one bitmap
	// container whose cardinality header is overwritten with header.
	newCorrupt := func(t *testing.T, real uint64, header int) *Bitmap {
		t.Helper()
		a := NewBitmap()
		for x := base; x < base+3000; x++ {
			a.Set(x) // force a bitmap container
		}
		for x := base + real; x < base+3000; x++ {
			a.Remove(x) // containers never down-convert: stays bitmap
		}
		off, found := a.keys.getValue(base)
		if !found {
			t.Fatal("missing container")
		}
		c := a.getContainer(off)
		if c[indexType] != typeBitmap {
			t.Fatal("fixture: container must be a bitmap container")
		}
		setCardinality(c, header)
		return a
	}

	t.Run("matched array overlapping more than the header claims", func(t *testing.T) {
		// the shape that drives a header-seeded count negative: pass-1 sizing
		// must not panic and the result must keep the real survivors
		a := newCorrupt(t, 3000, 100)
		b := NewBitmap()
		for x := base; x < base+500; x++ {
			b.Set(x) // matched array container, 500 hits > header's 100
		}
		res := AndNot(a, b)
		for _, x := range []uint64{0, 499, 500, 1234, 2999} {
			if got, want := res.Contains(base+x), x >= 500; got != want {
				t.Fatalf("Contains(base+%d) = %v, want %v", x, got, want)
			}
		}
	})

	t.Run("matched empty array container", func(t *testing.T) {
		a := newCorrupt(t, 3000, 100)
		b := NewBitmap()
		b.Set(base + 1)
		b.Remove(base + 1) // matched key with an empty container
		b.Set(5 << 16)     // keep b non-empty so AndNot doesn't Clone
		res := AndNot(a, b)
		for _, x := range []uint64{0, 100, 2999} {
			if !res.Contains(base + x) {
				t.Fatalf("membership lost at base+%d", x)
			}
		}
	})

	t.Run("unmatched container, dense real popcount", func(t *testing.T) {
		a := newCorrupt(t, 3000, 100)
		b := NewBitmap()
		b.Set(5 << 16) // no key overlap with a's corrupt container
		res := AndNot(a, b)
		for _, x := range []uint64{0, 100, 2999} {
			if !res.Contains(base + x) {
				t.Fatalf("membership lost at base+%d", x)
			}
		}
	})

	t.Run("unmatched container, sparse real popcount", func(t *testing.T) {
		a := newCorrupt(t, 500, 50)
		b := NewBitmap()
		b.Set(5 << 16)
		res := AndNot(a, b)
		off, found := res.keys.getValue(base)
		if !found {
			t.Fatal("container missing from result")
		}
		if c := res.getContainer(off); c[indexType] != typeArray {
			t.Fatalf("sparse corrupt container not compacted (type %d)", c[indexType])
		}
		if got := res.GetCardinality(); got != 500 {
			t.Fatalf("cardinality %d, want 500 (compaction repairs the header)", got)
		}
		for x := uint64(0); x < 500; x++ {
			if !res.Contains(base + x) {
				t.Fatalf("missing base+%d", x)
			}
		}
		if res.Contains(base + 600) {
			t.Fatal("false positive")
		}
	})

	t.Run("overstated header routes to dense path", func(t *testing.T) {
		a := newCorrupt(t, 100, 3000)
		b := NewBitmap()
		for x := base; x < base+5000; x++ {
			b.Set(x) // >4096 values: bitmap container
		}
		for x := base + 50; x < base+5000; x++ {
			b.Remove(x) // card 50 keeps a's header-dense check true
		}
		// header-dense (3000-50): the in-place build's bitmap-bc branch
		// popcounts for real, repairing the header
		res := AndNot(a, b)
		if got := res.GetCardinality(); got != 50 {
			t.Fatalf("cardinality %d, want 50", got)
		}
		for _, x := range []uint64{0, 49, 50, 99, 100} {
			if got, want := res.Contains(base+x), x >= 50 && x < 100; got != want {
				t.Fatalf("Contains(base+%d) = %v, want %v", x, got, want)
			}
		}
	})
}

// Sparse bitmap containers of a with no matching key in b must be compacted
// exactly like matched ones — the space win must not depend on b's key set.
func TestAndNotCompactsUnmatchedContainers(t *testing.T) {
	a := NewBitmap()
	for x := uint64(0); x < 3000; x++ {
		a.Set(x) // key-0 container converts to bitmap
	}
	for x := uint64(500); x < 3000; x++ {
		a.Remove(x) // 500 remain; containers never down-convert, stays bitmap
	}
	for x := uint64(1 << 16); x < 1<<16+5000; x++ {
		a.Set(x) // second container so b can match something else
	}
	if off, ok := a.keys.getValue(0); !ok || a.getContainer(off)[indexType] != typeBitmap {
		t.Fatal("fixture: key-0 container must be a sparse bitmap container")
	}
	b := NewBitmap()
	for x := uint64(1 << 16); x < 1<<16+2500; x++ {
		b.Set(x) // matches only container 1; container 0 is unmatched
	}
	res := AndNot(a, b)
	off, found := res.keys.getValue(0)
	if !found {
		t.Fatal("unmatched container missing from result")
	}
	if c := res.getContainer(off); c[indexType] != typeArray {
		t.Fatalf("unmatched sparse bitmap container not compacted (type %d, size %d)", c[indexType], len(c))
	}
	for x := uint64(0); x < 500; x++ {
		if !res.Contains(x) {
			t.Fatalf("missing %d", x)
		}
	}
	if res.Contains(600) {
		t.Fatal("false positive")
	}
}

// Two-pass sizing must grow res exactly once: allocations per AndNot call stay
// constant regardless of container count (res struct + initial data + one
// reservation + scratch), instead of one doubling per ~container.
func TestAndNotAllocsBounded(t *testing.T) {
	a := NewBitmap()
	for x := uint64(0); x < 2_000_000; x++ {
		a.Set(x) // ~30 full bitmap containers
	}
	b := NewBitmap()
	for x := uint64(0); x < 2_000_000; x += 3 {
		b.Set(x) // remove a third: results stay bitmap containers
	}
	allocs := testing.AllocsPerRun(10, func() {
		res := AndNot(a, b)
		if res.GetCardinality() == 0 {
			t.Fatal("unexpected empty result")
		}
	})
	// NewBitmap (struct+data), one exact reservation, one 8KB scratch, plus
	// small constant slack.
	if allocs > 8 {
		t.Fatalf("AndNot allocations not bounded: %.0f per call", allocs)
	}
}

// Heavy-shrink AndNot: dense minuend, subtrahend that removes almost everything —
// the regime where exact sizing and compaction matter most.
func BenchmarkAndNotHeavyShrink(b *testing.B) {
	a := NewBitmap()
	for x := uint64(0); x < 2_000_000; x++ {
		a.Set(x)
	}
	sub := NewBitmap()
	rng := rand.New(rand.NewSource(9))
	for x := uint64(0); x < 2_000_000; x++ {
		if rng.Intn(100) != 0 { // keep ~1%
			sub.Set(x)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	var s int
	for i := 0; i < b.N; i++ {
		s += AndNot(a, sub).GetCardinality()
	}
	sink = s
}

var sink int

// Dense AndNot: results stay bitmap containers — the regime where the sizing
// pass short-circuits and pass 2 builds containers directly inside res.
func BenchmarkAndNotDense(b *testing.B) {
	a := NewBitmap()
	for x := uint64(0); x < 2_000_000; x++ {
		a.Set(x)
	}
	sub := NewBitmap()
	for x := uint64(0); x < 2_000_000; x += 3 {
		sub.Set(x)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var s int
	for i := 0; i < b.N; i++ {
		s += AndNot(a, sub).GetCardinality()
	}
	sink = s
}
