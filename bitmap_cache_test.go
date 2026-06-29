package sroar

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// Tests for the Set last-container cache (cacheKey/cacheOff/cacheValid in
// Bitmap), which skips the key binary search on repeated/clustered Set calls.
// Its correctness rests on a fragile contract: every operation that can change a
// key's container offset must clear cacheValid. A missed invalidation would let
// Set read a stale offset and write an element into the wrong container — silent
// corruption that no pre-existing test catches.
//
// This file holds three layers of coverage:
//   - assertBitmapStructure / assertBitmapContents: invariant checkers,
//   - TestSetCacheDifferentialFuzz: a randomized op stream vs a map oracle,
//   - TestSetCache*: targeted regression guards, one per invalidation path.

// assertBitmapStructure verifies internal layout invariants that a corrupted
// Set cache would violate, independent of any oracle: keys strictly ascending,
// every container offset past the key header and within data, a valid container
// type, and no two containers overlapping (a stale offset can make Set write
// past one container into its neighbour).
func assertBitmapStructure(t *testing.T, ra *Bitmap) {
	t.Helper()

	n := ra.keys.numKeys()
	// The key node aliases the front of ra.data: ra.data[:len(keys)*4] (u64->u16).
	// Containers therefore start at offsets >= headerEnd.
	headerEnd := uint64(len(ra.keys) * 4)
	dataLen := uint64(len(ra.data))

	type span struct{ start, end uint64 }
	spans := make([]span, 0, n)

	var prevKey uint64
	for i := 0; i < n; i++ {
		key := ra.keys.key(i)
		off := ra.keys.val(i)

		if key&0xFFFF != 0 {
			t.Fatalf("key[%d]=%#x has non-zero low 16 bits", i, key)
		}
		if i > 0 && key <= prevKey {
			t.Fatalf("keys not strictly ascending at i=%d: %#x after %#x", i, key, prevKey)
		}
		prevKey = key

		if off < headerEnd || off >= dataLen {
			t.Fatalf("offset %d for key %#x outside container region [%d,%d)",
				off, key, headerEnd, dataLen)
		}
		c := ra.getContainer(off)
		sz := uint64(c[indexSize])
		if sz == 0 || off+sz > dataLen {
			t.Fatalf("container key=%#x off=%d size=%d exceeds data len %d", key, off, sz, dataLen)
		}
		if ty := c[indexType]; ty != typeArray && ty != typeBitmap {
			t.Fatalf("container key=%#x off=%d has invalid type %d", key, off, ty)
		}
		spans = append(spans, span{off, off + sz})
	}

	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	for i := 1; i < len(spans); i++ {
		if spans[i].start < spans[i-1].end {
			t.Fatalf("overlapping containers: [%d,%d) and [%d,%d)",
				spans[i-1].start, spans[i-1].end, spans[i].start, spans[i].end)
		}
	}
}

// assertBitmapContents is the definitive corruption check: ra's logical contents
// must equal want exactly. A stale cached offset surfaces here as a value in
// ToArray (reconstructed as key|low) that the oracle never inserted.
func assertBitmapContents(t *testing.T, ra *Bitmap, want map[uint64]struct{}) {
	t.Helper()

	if got := ra.GetCardinality(); got != len(want) {
		t.Fatalf("cardinality %d != oracle size %d", got, len(want))
	}
	got := ra.ToArray()
	wantArr := make([]uint64, 0, len(want))
	for v := range want {
		wantArr = append(wantArr, v)
	}
	sort.Slice(wantArr, func(i, j int) bool { return wantArr[i] < wantArr[j] })

	if len(got) != len(wantArr) {
		t.Fatalf("ToArray len %d != oracle len %d", len(got), len(wantArr))
	}
	for i := range got {
		if got[i] != wantArr[i] {
			t.Fatalf("element %d mismatch: got %#x want %#x", i, got[i], wantArr[i])
		}
	}
}

// offsetOf returns the current container offset for key, reading the real key
// node (not the cache).
func offsetOf(t *testing.T, ra *Bitmap, key uint64) uint64 {
	t.Helper()
	off, has := ra.keys.getValue(key)
	if !has {
		t.Fatalf("key %#x not present", key)
	}
	return off
}

// TestSetCacheDifferentialFuzz drives a randomized stream of mutating operations
// against a map oracle. The Set cache only ever holds the most-recently-Set key,
// so a stale offset can only surface as the sequence
//
//	Set(K)            -> cache = (K, offK)
//	<non-Set mutation> -> may move K's container; must invalidate
//	Set(K)            -> reads the cache
//
// so every iteration drives exactly that window on a "hot" key, wrapped around a
// randomly chosen structural mutation. Layout invariants are checked every step;
// the full content comparison runs periodically and at the end.
func TestSetCacheDifferentialFuzz(t *testing.T) {
	seeds := []int64{1, 7, 42, 1337, 99991}
	steps := 3000
	const checkpoint = 25 // full ToArray-vs-oracle comparison cadence
	if testing.Short() {
		seeds = seeds[:2]
		steps = 600
	}

	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))

			// Small key universe so containers are reused and the cache actually
			// hits. Key 0 is "dense" (full low range) so it grows into a bitmap
			// container, exercising the array->bitmap conversion under a warm
			// cache; the rest stay sparse arrays.
			const numKeys = 16
			lowRange := func(k uint64) int {
				if k == 0 {
					return maxCardinality
				}
				return 256
			}
			mkVal := func(k uint64) uint64 { return k<<16 | uint64(rng.Intn(lowRange(k))) }
			randVal := func() uint64 { return mkVal(uint64(rng.Intn(numKeys))) }

			randBitmap := func() (*Bitmap, map[uint64]struct{}) {
				b := NewBitmap()
				m := map[uint64]struct{}{}
				cnt := rng.Intn(120)
				for i := 0; i < cnt; i++ {
					v := randVal()
					b.Set(v)
					m[v] = struct{}{}
				}
				return b, m
			}

			ra := NewBitmap()
			oracle := map[uint64]struct{}{}
			hotKey := uint64(rng.Intn(numKeys))

			for step := 0; step < steps; step++ {
				// Warm the cache on hotKey.
				v0 := mkVal(hotKey)
				ra.Set(v0)
				oracle[v0] = struct{}{}

				// A randomly chosen mutation that may move hotKey's container.
				switch rng.Intn(10) {
				case 0, 1, 2: // plain Set elsewhere
					v := randVal()
					ra.Set(v)
					oracle[v] = struct{}{}
				case 3: // Remove a single element
					v := randVal()
					ra.Remove(v)
					delete(oracle, v)
				case 4: // RemoveRange within a single key
					k := uint64(rng.Intn(numKeys))
					a := uint64(rng.Intn(maxCardinality))
					b := uint64(rng.Intn(maxCardinality))
					if a > b {
						a, b = b, a
					}
					lo, hi := k<<16|a, k<<16|b
					ra.RemoveRange(lo, hi)
					for x := range oracle {
						if x >= lo && x < hi {
							delete(oracle, x)
						}
					}
				case 5, 6: // Or (union)
					b, mb := randBitmap()
					ra.Or(b)
					for x := range mb {
						oracle[x] = struct{}{}
					}
				case 7: // And (intersection)
					b, mb := randBitmap()
					ra.And(b)
					for x := range oracle {
						if _, ok := mb[x]; !ok {
							delete(oracle, x)
						}
					}
				case 8: // AndNot (difference)
					b, mb := randBitmap()
					ra.AndNot(b)
					for x := range mb {
						delete(oracle, x)
					}
				case 9: // Clone / buffer roundtrip — the cache must not survive
					if rng.Intn(2) == 0 {
						ra = ra.Clone()
					} else {
						ra = FromBuffer(ra.ToBufferWithCopy())
					}
				}

				// Re-touch hotKey via Set OR Remove: this reads the cache if the
				// mutation above failed to invalidate it. Alternating the
				// operation exercises Remove (not just Set) as the cache consumer
				// in the critical window.
				v1 := mkVal(hotKey)
				if rng.Intn(2) == 0 {
					ra.Set(v1)
					oracle[v1] = struct{}{}
				} else {
					ra.Remove(v1)
					delete(oracle, v1)
				}

				assertBitmapStructure(t, ra)
				if got := ra.GetCardinality(); got != len(oracle) {
					t.Fatalf("step %d: cardinality %d != oracle %d", step, got, len(oracle))
				}
				if step%checkpoint == 0 {
					assertBitmapContents(t, ra, oracle)
				}

				// Rotate the hot key so different container shapes (the dense
				// bitmap vs sparse arrays, different positions) get exercised.
				if rng.Intn(20) == 0 {
					hotKey = uint64(rng.Intn(numKeys))
				}
			}
			assertBitmapContents(t, ra, oracle)
		})
	}
}

// The targeted tests below are regression guards for the cache, one per
// offset-changing path that must clear cacheValid. Each test:
//
//  1. warms the cache on a key K (a Set on K leaves cache = (K, offK)),
//  2. runs a NON-Set operation that moves K's container (back-to-back Sets
//     can't leave a stale cache, since each Set overwrites the cache key),
//  3. asserts the move actually happened (so the scenario keeps exercising the
//     path even after future refactors — a self-check, not the real assertion),
//  4. Sets K again and asserts the element landed in K's real container.
//
// If the invalidation on the exercised path were removed, step 4's Set would
// write to the stale offset and the Contains check would fail.

// TestSetCacheStaleAfterOrAddsNewKeys exercises the expandConditionally path:
// Or-ing in a bitmap with brand-new keys grows the key region, scooting every
// existing container (including the cached one) to the right.
//
// A key is the high 48 bits of a value (the container selector); the elements
// of that container are key|0, key|1, ... Keys below are declared already
// shifted into place, so an element is just key|low and a lookup needs no shift.
func TestSetCacheStaleAfterOrAddsNewKeys(t *testing.T) {
	const kKey = uint64(50) << 16
	ra := NewBitmap()
	for low := uint64(0); low < 8; low++ {
		ra.Set(kKey | low)
	}
	ra.Set(kKey | 1000) // warm the cache on kKey
	before := offsetOf(t, ra, kKey)

	// Many new lower keys -> key region must grow -> containers scoot right.
	b := NewBitmap()
	for k := uint64(0); k < 40; k++ {
		b.Set(k<<16 | 1)
	}
	ra.Or(b)

	after := offsetOf(t, ra, kKey)
	if after == before {
		t.Fatalf("scenario did not move kKey's container (off stayed %d); "+
			"no longer exercises expandConditionally invalidation", before)
	}

	val := kKey | 2000
	ra.Set(val)
	if !ra.Contains(val) {
		t.Fatal("Set wrote to stale cached offset after Or-grew key region (missing expandConditionally invalidation)")
	}
	assertBitmapStructure(t, ra)
}

// TestSetCacheStaleAfterOrOverflowRepoint exercises the setKey repoint path:
// Or overflowing the cached key's container slot appends a merged container at
// the end and repoints the key to it, without a scoot.
func TestSetCacheStaleAfterOrOverflowRepoint(t *testing.T) {
	const kKey = uint64(5) << 16
	ra := NewBitmap()
	// Several keys with snug ~12-element array containers.
	for k := uint64(0); k < 20; k++ {
		for low := uint64(0); low < 12; low++ {
			ra.Set(k<<16 | low)
		}
	}
	ra.Set(kKey | 0) // warm the cache on kKey
	before := offsetOf(t, ra, kKey)

	// b adds enough disjoint elements to every key that the merged result
	// overflows the existing slot (minContainerSize holds ~60), forcing Or's
	// append-and-forget path to repoint the key to a container at the end.
	b := NewBitmap()
	for k := uint64(0); k < 20; k++ {
		for low := uint64(100); low < 400; low++ {
			b.Set(k<<16 | low)
		}
	}
	ra.Or(b)

	after := offsetOf(t, ra, kKey)
	if after == before {
		t.Fatalf("scenario did not repoint kKey (off stayed %d); "+
			"no longer exercises setKey invalidation", before)
	}

	val := kKey | 500
	ra.Set(val)
	if !ra.Contains(val) {
		t.Fatal("Set wrote to orphaned container after Or repoint (missing setKey invalidation)")
	}
	assertBitmapStructure(t, ra)
}

// TestSetCacheStaleAfterRemoveRangeCleanup exercises the scootLeft path:
// fully emptying an earlier key's container makes Cleanup reclaim it and
// scootLeft the later (cached) container down.
func TestSetCacheStaleAfterRemoveRangeCleanup(t *testing.T) {
	const jKey, kKey = uint64(1) << 16, uint64(2) << 16
	ra := NewBitmap()
	for low := uint64(0); low < 200; low++ {
		ra.Set(jKey | low)
	}
	for low := uint64(0); low < 8; low++ {
		ra.Set(kKey | low)
	}
	ra.Set(kKey | 1000) // warm the cache on kKey
	before := offsetOf(t, ra, kKey)

	// Remove all of jKey -> its container empties -> Cleanup scootLefts kKey.
	ra.RemoveRange(jKey, jKey+(1<<16))

	after := offsetOf(t, ra, kKey)
	if after == before {
		t.Fatalf("scenario did not move kKey's container (off stayed %d); "+
			"no longer exercises scootLeft invalidation", before)
	}

	val := kKey | 2000
	ra.Set(val)
	if !ra.Contains(val) {
		t.Fatal("Set wrote to stale cached offset after RemoveRange/Cleanup scootLeft (missing invalidation)")
	}
	assertBitmapStructure(t, ra)
}

// TestSetCacheValidWhenLastContainerGrowsAndConverts exercises the keep-valid
// side of the contract: scootRight at end-of-data reports moved=false, so the
// cache stays valid. Growing (and converting array->bitmap) the LAST container
// in place must not move its offset, and the warm cache must keep pointing at
// it. Guards the scootRight "did it move" optimization — a wrong moved=true here
// would only waste work, but a wrong moved=false elsewhere would corrupt, so we
// pin the no-move case down.
func TestSetCacheValidWhenLastContainerGrowsAndConverts(t *testing.T) {
	const jKey, kKey = uint64(1) << 16, uint64(2) << 16
	ra := NewBitmap()
	for low := uint64(0); low < 8; low++ {
		ra.Set(jKey | low)
	}
	for low := uint64(0); low < 8; low++ {
		ra.Set(kKey | low) // kKey is the last key -> last container
	}
	ra.Set(kKey | 100) // warm the cache on the last container
	before := offsetOf(t, ra, kKey)

	// Grow kKey's own container well past the array->bitmap threshold. Each
	// expansion scoots at end-of-data (no tail) so the offset must not move.
	for low := uint64(200); low < 6000; low++ {
		ra.Set(kKey | low)
	}

	if after := offsetOf(t, ra, kKey); after != before {
		t.Fatalf("last container unexpectedly moved (%d -> %d)", before, after)
	}
	// Every element written through the warm cache must be present.
	for low := uint64(200); low < 6000; low++ {
		if !ra.Contains(kKey | low) {
			t.Fatalf("missing element %d after in-place grow+convert of last container", low)
		}
	}
	// jKey must be untouched.
	for low := uint64(0); low < 8; low++ {
		if !ra.Contains(jKey | low) {
			t.Fatalf("earlier container element %d corrupted", low)
		}
	}
	assertBitmapStructure(t, ra)
}

// TestRemovePrimesCacheForSet covers Remove priming the shared cache: emptying a
// key's container leaves it in place (Remove never reclaims), and the last
// Remove primes the cache onto that key. A following Set on the same key must
// reuse that Remove-primed offset to write into the (now empty) container. The
// cache is first primed on a different key via Set, so the Set on kKey can only
// hit the cache if Remove re-primed it — making the prime path deterministic.
func TestRemovePrimesCacheForSet(t *testing.T) {
	const otherKey, kKey = uint64(1) << 16, uint64(7) << 16
	ra := NewBitmap()
	for low := uint64(0); low < 5; low++ {
		ra.Set(otherKey | low)
	}
	for low := uint64(0); low < 5; low++ {
		ra.Set(kKey | low)
	}
	kOff := offsetOf(t, ra, kKey)

	// Prime the cache on otherKey via Set, so a later cache hit on kKey can only
	// come from Remove having re-primed it.
	ra.Set(otherKey | 50)

	// Empty kKey with Remove. Remove leaves the (now empty) container in place
	// and primes the cache onto kKey.
	for low := uint64(0); low < 5; low++ {
		ra.Remove(kKey | low)
	}
	if off, has := ra.keys.getValue(kKey); !has || off != kOff {
		t.Fatalf("Remove moved/removed kKey's container (off %d->%d has=%v); "+
			"test assumes Remove leaves it in place", kOff, off, has)
	}
	// Directly assert Remove primed the cache onto kKey (without this the test
	// would still pass via the getValue fallback, not guarding the prime).
	if !ra.cacheValid || ra.cacheKey != kKey || ra.cacheOff != kOff {
		t.Fatalf("Remove did not prime the cache onto kKey: valid=%v key=%#x off=%d, want key=%#x off=%d",
			ra.cacheValid, ra.cacheKey, ra.cacheOff, kKey, kOff)
	}

	// Set on kKey reuses the Remove-primed cache to write into the emptied
	// container.
	val := kKey | 42
	ra.Set(val)
	if !ra.Contains(val) {
		t.Fatal("Set wrote to wrong container via a bad Remove-primed cache")
	}
	if ra.Contains(kKey | 0) {
		t.Fatal("a removed element resurfaced")
	}
	for low := uint64(0); low < 5; low++ {
		if !ra.Contains(otherKey | low) {
			t.Fatalf("neighbour element %d corrupted", low)
		}
	}
	assertBitmapStructure(t, ra)
}
