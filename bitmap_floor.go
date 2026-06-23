package sroar

import (
	"math/bits"
)

// FloorMasked returns a new bitmap holding, for each bit v in vals, the
// greatest bit c ≤ v in candidates such that (v & mask) == (c & mask). Bits
// in vals whose group has no predecessor in candidates contribute nothing.
// The result is naturally deduplicated (multiple v's mapping to the same c
// emit c once).
//
// The mask defines a grouping over values: two values belong to the same group
// iff they agree under mask. Within a group, the operation is a classic
// predecessor (floor) query; groups are independent.
//
// The lowest 16 bits of mask are always zeroed: they correspond to the
// within-container offset, not the container key, and grouping is only
// meaningful at container-key granularity.
//
// Neither input is modified.
func FloorMasked(vals, candidates *Bitmap, mask uint64) *Bitmap {
	return floorMaskedInto(vals, candidates, mask, NewBitmap())
}

// FloorMaskedToBuf is like [FloorMasked] but uses the provided byte slice as
// the underlying buffer for the result bitmap, avoiding the result's heap
// allocation when the buffer is large enough.
//
// Sizing guidance: a safe upper bound is candidates.LenInBytes() — the result
// has at most one bit per bit of candidates in matching groups, and at most
// one result container per candidates container, so it can never exceed
// candidates in storage.
func FloorMaskedToBuf(vals, candidates *Bitmap, mask uint64, buf []byte) *Bitmap {
	return floorMaskedInto(vals, candidates, mask, NewBitmapToBuf(buf))
}

func floorMaskedInto(vals, candidates *Bitmap, mask uint64, res *Bitmap) *Bitmap {
	if vals.IsEmpty() || candidates.IsEmpty() {
		return res
	}

	// Precondition (caller commitment): mask & 0xFFFF == 0xFFFF. The per-bit
	// step is a membership test of vals' low 16 bits against the matching
	// candidates container — i.e. equality is the only candidate within a
	// container-key group. Other low-16 mask shapes are undefined; the impl
	// treats them as 0xFFFF.
	mask &= 0xFFFFFFFFFFFF0000

	// Masked keys are not guaranteed to be ascending in raw-key order — for
	// non-contiguous-high masks (e.g. 0x0000FFFFFFFF0000, common when the
	// high bits encode an auxiliary identifier that the mask strips), two
	// raw keys k1 < k2 can have k1&mask > k2&mask. So we cannot walk masked
	// groups by stepping through raw keys; we have to sort entries by
	// (maskedKey, originalKey) first.
	aEntries := buildKeyedMaskedEntries(vals, mask)
	bEntries := buildKeyedMaskedEntries(candidates, mask)

	res.expandConditionally(len(bEntries), 0)

	// Find the largest masked-group size in bEntries — we'll allocate
	// per-bucket storage sized to this. The buckets are reused across
	// every processFloorGroup call (one alloc for the whole FloorMasked
	// call), so sizing to the largest group ensures it's always enough.
	maxBGroupSize := 0
	for i := 0; i < len(bEntries); {
		j := i + 1
		for j < len(bEntries) && bEntries[j].maskedKey == bEntries[i].maskedKey {
			j++
		}
		if j-i > maxBGroupSize {
			maxBGroupSize = j - i
		}
		i = j
	}
	if maxBGroupSize == 0 {
		return res
	}

	// arrBuf: stack-allocated scratch for floorWriteBucket's array-form
	// serialization (only used on the small-card flush path).
	var arrBuf [maxContainerSize]uint16

	// Per-bucket storage, one bucket per candidates entry in the current
	// masked group. The key insight that justifies this design:
	// bGroup[i].originalKey is globally unique across every processFloorGroup
	// call (each candidates container has a unique container key), so each
	// masked-group's flush writes to a fresh key in res. No OR-merge ever
	// fires, and bucketing within one processFloorGroup means each vals
	// container's non-monotonic emit pattern (cache hits scattered across
	// candidates entries) gets sorted by bucket into N small buffers —
	// N flushes per group instead of N × vals_container_card.
	//
	// bucketBufs is a single contiguous []uint16, viewed as maxBGroupSize
	// bitmap-form buffers of size maxContainerSize each. bucketStates tracks
	// per-bucket card/minWord/maxWord for compact flush.
	bucketBufs := make([]uint16, maxBGroupSize*maxContainerSize)
	bucketStates := make([]floorBucketState, maxBGroupSize)

	ai, bi := 0, 0
	an, bn := len(aEntries), len(bEntries)

	for ai < an && bi < bn {
		ag := aEntries[ai].maskedKey
		bg := bEntries[bi].maskedKey

		if ag < bg {
			ai++
			for ai < an && aEntries[ai].maskedKey == ag {
				ai++
			}
			continue
		}
		if ag > bg {
			bi++
			for bi < bn && bEntries[bi].maskedKey == bg {
				bi++
			}
			continue
		}

		aiEnd := ai + 1
		for aiEnd < an && aEntries[aiEnd].maskedKey == ag {
			aiEnd++
		}
		biEnd := bi + 1
		for biEnd < bn && bEntries[biEnd].maskedKey == bg {
			biEnd++
		}

		processFloorGroup(vals, candidates, aEntries[ai:aiEnd], bEntries[bi:biEnd], res, bucketBufs, bucketStates, arrBuf[:])
		ai = aiEnd
		bi = biEnd
	}

	return res
}

// floorCacheHalf is the number of cache slots in each half (cacheLo / cacheHi).
// The full per-low-bit cache spans maxCardinality (= 65536) slots — one per
// possible low-16-bit value within a container; we split in two so each half
// fits under Go's MaxStackVarSize (128 KB) and stack-allocates. Each half is
// maxCardinality/2 * 4 bytes = 128 KB exactly.
const floorCacheHalf = maxCardinality / 2

// floorArrMaxCard is the cardinality threshold at which floorWriteBucket
// switches from array-form to bitmap-form output. Above this, the result
// container is written as a full bitmap (maxContainerSize uint16s)
// regardless of how it would fit as an array.
//
// Set to half of bitmapDataWords (= 2048), matching sroar's stepSize
// convention — sroar's organic array-container growth tops out at size
// 2048 (= ~2044 elements) before promoting to bitmap.
const floorArrMaxCard = bitmapDataWords / 2

// floorBucketState tracks per-bucket accumulation state inside one
// processFloorGroup call. Each bucket holds the emitted bits destined for a
// single b entry's originalKey (= the eventual key in res). card / minWord /
// maxWord support the bucket's flush at end-of-group: card chooses array-
// vs bitmap-form output, minWord/maxWord bound the array-form scan and the
// targeted clear of the bucket's scratch.
//
// Field widths reflect actual value ranges:
//   - card: 0..maxCardinality (= 65536) → uint32.
//   - minWord: 0..bitmapDataWords (= 4096; the upper bound is the reset
//     sentinel for "no bits set yet") → uint16.
//   - maxWord: 0..bitmapDataWords-1 (= 4095) → uint16. Reset to 0; the
//     `if wordIdx > state.maxWord` update leaves maxWord at 0 when the
//     first bit lands in word 0, which is the correct answer (max IS 0).
type floorBucketState struct {
	card    uint32
	minWord uint16
	maxWord uint16
}

// processFloorGroup handles one masked group: for every bit v in the vals
// entries of this group, find the greatest bit c ≤ v in the candidates
// entries of this group and emit c into res. Entries in aGroup and bGroup
// share the same masked container key and are sorted ascending by
// originalKey; within a container, values are sorted ascending.
// buildKeyedMaskedEntries has already stripped empty containers.
//
// Algorithm: parallel walk over a flat 65536-slot per-low-bit cache (split
// across two stack-allocated [32768]uint32 halves to fit under Go's
// MaxStackVarSize = 128 KB per explicit variable), with per-bucket
// accumulation.
//
//   - As candidates containers are processed, floorUpdateCache writes
//     (bi+1) into the right cache half for every set low-16-bit value in
//     the container. The +1 keeps 0 as a sentinel for "no candidates
//     entry has covered this low value yet".
//   - As vals containers are processed, emitFloorsBucketed looks up the
//     cache for each set bit's low-16 value and, if the slot is non-zero,
//     writes the set bit into bucket[slot-1] inside bucketBufs.
//   - At end of the masked group, each non-empty bucket is flushed once
//     via floorWriteBucket. No OR-merge ever fires: each candidates
//     entry's originalKey is unique across every processFloorGroup call,
//     so every flush hits a fresh key in res.
//
// Why bucketing: an alternative design (single accumulator, flush on
// destination-key change) would suffer O(N × vals_container_card) flushes
// per vals container under the cache algorithm's non-monotonic emit
// pattern (a single vals container can resolve different low values to
// different candidates entries, and consecutive vals containers can
// re-resolve to earlier candidates entries → frequent destination changes
// → frequent flush → repeated OR-merges into the same res container).
// Bucketing classifies emits by destination key on the fly and flushes
// each destination once, dropping flush count from O(card) to O(|bGroup|)
// per vals container.
//
// Tie-break: at equal originalKey, the candidates entry is processed
// first so the vals entry sees the cache updates before its own emission.
// This covers the case where a bit in vals is its own floor because that
// same value also appears in candidates at the same container key.
//
// Go zero-inits the cache halves per call so sentinel-0 = "unset" without
// explicit clear. The closure-free signatures of floorUpdateCache and
// emitFloorsBucketed are load-bearing: a closure capturing the cache
// halves would force them to the heap. See go build -gcflags='-m=2' as a
// merge-gate verification.
func processFloorGroup(
	a, b *Bitmap,
	aGroup, bGroup []keyedMaskedEntry,
	res *Bitmap,
	bucketBufs []uint16,
	bucketStates []floorBucketState,
	arrBuf []uint16,
) {
	var cacheLo, cacheHi [floorCacheHalf]uint32

	nBuckets := len(bGroup)

	// Reset per-bucket state for this masked group. bucketBufs's touched
	// words are cleared at the end of the previous group's flush (see the
	// flush loop below), so here we only need to reset the scalar state.
	for i := 0; i < nBuckets; i++ {
		bucketStates[i].card = 0
		bucketStates[i].minWord = bitmapDataWords
		bucketStates[i].maxWord = 0
	}

	ai, bi := 0, 0
	an, bn := len(aGroup), len(bGroup)

	for ai < an && bi < bn {
		if bGroup[bi].originalKey <= aGroup[ai].originalKey {
			bc := b.getContainer(bGroup[bi].offset)
			floorUpdateCache(bc, &cacheLo, &cacheHi, uint32(bi)+1)
			bi++
		} else {
			ac := a.getContainer(aGroup[ai].offset)
			emitFloorsBucketed(ac, &cacheLo, &cacheHi, bucketBufs, bucketStates)
			ai++
		}
	}
	// Drain remaining vals entries — the cache already holds the latest
	// candidates entry for every low value that ever appeared in this
	// group's candidates entries. Remaining candidates entries can't
	// affect emission since no vals entry is left.
	for ai < an {
		ac := a.getContainer(aGroup[ai].offset)
		emitFloorsBucketed(ac, &cacheLo, &cacheHi, bucketBufs, bucketStates)
		ai++
	}

	// Flush each non-empty bucket to res. Each destination key is unique
	// across the whole call, so every flush is a fresh container write —
	// no OR-merge needed.
	si := int(startIdx)
	for k := 0; k < nBuckets; k++ {
		state := &bucketStates[k]
		if state.card == 0 {
			continue
		}
		bucketStart := k * maxContainerSize
		bucketBuf := bucketBufs[bucketStart : bucketStart+maxContainerSize]
		floorWriteBucket(res, bGroup[k].originalKey, bucketBuf, state.card, state.minWord, state.maxWord, arrBuf)
		// Clear only the words this bucket touched so the next masked group
		// (which reuses bucketBufs) starts from a clean slate. The rest of
		// bucketBufs is zero from the initial make().
		clear(bucketBuf[si+int(state.minWord) : si+int(state.maxWord)+1])
	}
}

// floorUpdateCache writes slot into the right cache half for every set bit's
// low-16 value in c (a candidates container). Dispatches on container type.
// For bitmap containers, the outer uint64-stride loop skips 4 uint16 words
// at a time over empty regions; because each uint64 spans 64 consecutive
// low values, all bits in one uint64 land in the same cache half — so the
// cache-half dispatch happens once per uint64, not per bit.
func floorUpdateCache(c []uint16, cacheLo, cacheHi *[floorCacheHalf]uint32, slot uint32) {
	si := int(startIdx)
	if c[indexType] == typeArray {
		n := getCardinality(c)
		for i := 0; i < n; i++ {
			low := c[si+i]
			if low < floorCacheHalf {
				cacheLo[low] = slot
			} else {
				cacheHi[low-floorCacheHalf] = slot
			}
		}
		return
	}
	c64 := uint16To64SliceUnsafe(c[si:])
	const halfStride = bitmapDataUint64s / 2
	for j := 0; j < bitmapDataUint64s; j++ {
		if c64[j] == 0 {
			continue
		}
		cache := cacheLo
		var baseAdjust uint16
		if j >= halfStride {
			cache = cacheHi
			baseAdjust = floorCacheHalf
		}
		for k := 0; k < 4; k++ {
			w := j*4 + k
			word := c[si+w]
			if word == 0 {
				continue
			}
			base := uint16(w*16) - baseAdjust
			for word != 0 {
				pos := bits.LeadingZeros16(word)
				word &^= 1 << uint(15-pos)
				cache[base+uint16(pos)] = slot
			}
		}
	}
}

// emitFloorsBucketed looks up the cache for every set bit's low-16 value in c
// (a vals container); if the slot is non-zero, sets the corresponding bit in
// bucketBufs[(slot-1)*maxContainerSize ..] and updates that bucket's
// card / minWord / maxWord. No emission to a shared accumulator — every bit
// goes straight to its target bucket.
//
// Container-type dispatch mirrors floorUpdateCache; the bitmap path uses
// the per-uint64 cache-half pick to keep per-bit overhead minimal.
func emitFloorsBucketed(c []uint16, cacheLo, cacheHi *[floorCacheHalf]uint32, bucketBufs []uint16, bucketStates []floorBucketState) {
	si := int(startIdx)
	if c[indexType] == typeArray {
		n := getCardinality(c)
		for i := 0; i < n; i++ {
			low := c[si+i]
			var slot uint32
			if low < floorCacheHalf {
				slot = cacheLo[low]
			} else {
				slot = cacheHi[low-floorCacheHalf]
			}
			if slot == 0 {
				continue
			}
			floorSetBucketBit(bucketBufs, bucketStates, int(slot-1), low)
		}
		return
	}
	c64 := uint16To64SliceUnsafe(c[si:])
	const halfStride = bitmapDataUint64s / 2
	for j := 0; j < bitmapDataUint64s; j++ {
		if c64[j] == 0 {
			continue
		}
		cache := cacheLo
		var baseAdjust uint16
		if j >= halfStride {
			cache = cacheHi
			baseAdjust = floorCacheHalf
		}
		for k := 0; k < 4; k++ {
			w := j*4 + k
			word := c[si+w]
			if word == 0 {
				continue
			}
			base := uint16(w*16) - baseAdjust
			for word != 0 {
				pos := bits.LeadingZeros16(word)
				word &^= 1 << uint(15-pos)
				cacheIdx := base + uint16(pos)
				if slot := cache[cacheIdx]; slot != 0 {
					floorSetBucketBit(bucketBufs, bucketStates, int(slot-1), cacheIdx+baseAdjust)
				}
			}
		}
	}
}

// floorSetBucketBit sets the bit for low in bucket bIdx and updates the
// bucket's card / minWord / maxWord. Dedup is natural (setting the same
// bit twice is a no-op but skips the card++).
func floorSetBucketBit(bucketBufs []uint16, bucketStates []floorBucketState, bIdx int, low uint16) {
	wordIdx := low >> 4
	bufIdx := bIdx*maxContainerSize + int(startIdx) + int(wordIdx)
	bit := bitmapMask[low&0xF]
	if bucketBufs[bufIdx]&bit != 0 {
		return
	}
	bucketBufs[bufIdx] |= bit
	state := &bucketStates[bIdx]
	state.card++
	if wordIdx < state.minWord {
		state.minWord = wordIdx
	}
	if wordIdx > state.maxWord {
		state.maxWord = wordIdx
	}
}

// floorWriteBucket writes a flushed bucket to res at the given key. The key
// is always fresh in res (each b entry's originalKey is unique across the
// whole FloorMasked call), so no OR-merge logic is needed. Chooses array-
// form output for compact small-card containers (card ≤ floorArrMaxCard) and
// bitmap-form for larger.
//
// arrBuf is the array-form serialization scratch (only touched on the
// small-card path). bmpSrc is the bucket's bitmap-form buffer; the header
// is set in place on the bitmap-form path.
func floorWriteBucket(res *Bitmap, key uint64, bmpSrc []uint16, card uint32, minWord, maxWord uint16, arrBuf []uint16) {
	si := int(startIdx)
	var src []uint16
	if card > floorArrMaxCard {
		bmpSrc[indexSize] = maxContainerSize
		bmpSrc[indexType] = typeBitmap
		setCardinality(bmpSrc, int(card))
		src = bmpSrc
	} else {
		// Materialize array form by scanning only the touched word range.
		idx := si
		for w := minWord; w <= maxWord; w++ {
			word := bmpSrc[si+int(w)]
			if word == 0 {
				continue
			}
			base := w * 16
			for word != 0 {
				pos := bits.LeadingZeros16(word)
				word &^= 1 << uint(15-pos)
				arrBuf[idx] = base + uint16(pos)
				idx++
			}
		}
		arrBuf[indexSize] = uint16(idx)
		arrBuf[indexType] = typeArray
		setCardinality(arrBuf, int(card))
		src = arrBuf[:idx]
	}
	res.expandConditionally(0, len(src))
	off := res.newContainerNoClr(uint16(len(src)))
	copy(res.data[off:], src)
	res.setKey(key, off)
}
