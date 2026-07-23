package sroar

import (
	"fmt"
	"math"
	"slices"
	"sync"
)

// Intersects reports whether ra and bm share at least one element.
// Equivalent to !ra.Clone().And(bm).IsEmpty() but short-circuits at the
// first common element without allocating a result bitmap.
func (ra *Bitmap) Intersects(bm *Bitmap) bool {
	if ra.IsEmpty() || bm.IsEmpty() {
		return false
	}

	an := ra.keys.numKeys()
	bn := bm.keys.numKeys()
	useGallopA := shouldGallop(an, bn)
	useGallopB := shouldGallop(bn, an)

	// Start bi at the first bm key >= ra's first key.
	bi := bm.keys.search(ra.keys.key(0))

	for ai := 0; ai < an && bi < bn; {
		ak := ra.keys.key(ai)
		bk := bm.keys.key(bi)
		if ak == bk {
			ac := ra.getContainer(ra.keys.val(ai))
			bc := bm.getContainer(bm.keys.val(bi))
			if containerIntersects(ac, bc) {
				return true
			}
			ai++
			bi++
		} else if ak < bk {
			if useGallopA {
				ai = ra.keys.searchFrom(ai, bk)
			} else {
				ai++
			}
		} else {
			if useGallopB {
				bi = bm.keys.searchFrom(bi, ak)
			} else {
				bi++
			}
		}
	}
	return false
}

// IntersectsMasked reports whether ra and bm share at least one element after
// applying mask to bm's keys. Equivalent to !ra.Clone().And(bm.Masked(mask)).IsEmpty()
// but short-circuits at the first common element without allocating.
// Containers in ra whose key has a non-zero dimension (low 16 bits != 0) are
// not checked — same behaviour as AndMasked.
func (ra *Bitmap) IntersectsMasked(bm *Bitmap, mask uint64) bool {
	if ra.IsEmpty() || bm.IsEmpty() {
		return false
	}

	mask &= 0xFFFFFFFFFFFF0000
	entries := buildMaskedEntries(bm, mask)

	var from int
	var group []maskedEntry

	an := ra.keys.numKeys()
	for ai := 0; ai < an; ai++ {
		ak := ra.keys.key(ai)
		if uint16(ak) != 0 {
			continue
		}

		group, from = maskedEntriesFor(entries, ak, from)
		if group == nil {
			continue
		}

		ac := ra.getContainer(ra.keys.val(ai))
		for _, e := range group {
			bc := bm.getContainer(e.offset)
			if containerIntersects(ac, bc) {
				return true
			}
		}
	}
	return false
}

func And(a, b *Bitmap) *Bitmap {
	res := NewBitmap()
	if a.IsEmpty() || b.IsEmpty() {
		return res
	}

	andContainers(a, b, res, nil)
	return res
}

func andContainers(a, b, res *Bitmap, optBuf []uint16) {
	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			if c := containerAndAlt(ac, bc, optBuf, 0); len(c) > 0 && getCardinality(c) > 0 {
				// create a new container and update the key offset to this container.
				offset := res.newContainerNoClr(uint16(len(c)))
				copy(res.data[offset:], c)
				res.setKey(ak, offset)
			}
			ai++
			bi++
		} else if ak < bk {
			ai++
		} else {
			bi++
		}
	}
}

// And performs in-place AND of ra and bm (ra &= bm).
// ra drives the two-pointer walk: ra containers with no matching bm key must
// be zeroed out, so every ra container must be visited.
func (ra *Bitmap) And(bm *Bitmap) *Bitmap {
	if bm.IsEmpty() {
		ra.ZeroOut()
		return ra
	}

	an := ra.keys.numKeys()
	bn := bm.keys.numKeys()
	useGallopB := shouldGallop(bn, an)
	andContainersInRange(ra, bm, 0, an, nil, useGallopB)
	return ra
}

// AndConc performs And merge concurrently (ra &= bm).
// ra drives the walk: every ra container must be visited to zero out those
// with no matching bm key. Concurrency is calculated based on number of
// internal containers in ra, so that each goroutine handles at least
// [minContainersPerRoutine] containers.
// maxConcurrency limits concurrency calculated internally.
// If maxConcurrency <= 0, then calculated concurrency is not limited.
//
// E.g.: dst bitmap has 100 containers. Internal concurrency = 100/24 = 4. For:
// - maxConcurrency = 2, there will be 2 goroutines executed
// - maxConcurrency = 6, there will be 4 goroutines executed
func (ra *Bitmap) AndConc(bm *Bitmap, maxConcurrency int) *Bitmap {
	if bm.IsEmpty() {
		ra.ZeroOut()
		return ra
	}

	an := ra.keys.numKeys()
	bn := bm.keys.numKeys()
	concurrency := calcConcurrency(an, minContainersPerRoutine, maxConcurrency)
	useGallopB := shouldGallop(bn, an/concurrency)
	callback := func(ai, aj, _ int) { andContainersInRange(ra, bm, ai, aj, nil, useGallopB) }
	concurrentlyInRanges(an, concurrency, callback)
	return ra
}

// andContainersInRange ANDs a's containers in [ai, aj) with b in place.
// When useGallop is true, b's pointer advances via exponential search
// (searchFrom) instead of bi++, skipping large gaps in O(log gap).
// useGallop should be set when b has many more keys than a's range (b >> a).
func andContainersInRange(a, b *Bitmap, ai, aj int, optBuf []uint16, useGallopB bool) {
	ak := a.keys.key(ai)
	bi := b.keys.search(ak)
	bn := b.keys.numKeys()

	for ai < aj && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			if c := containerAndAlt(ac, bc, optBuf, runInline); len(c) > 0 {
				panic("new container not expected in And inline mode")
			}
			ai++
			bi++
		} else if ak < bk {
			off := a.keys.val(ai)
			zeroOutContainer(a.data[off:])
			ai++
		} else {
			if useGallopB {
				bi = b.keys.searchFrom(bi, ak)
			} else {
				bi++
			}
		}
	}
	for ; ai < aj; ai++ {
		off := a.keys.val(ai)
		zeroOutContainer(a.data[off:])
	}
}

// andNotCompactThreshold is the result cardinality below which AndNot writes a
// surviving bitmap container out as an array container. Set below the 4096
// encoding break-even on purpose: the conversion is only worth losing the O(1)
// bit-test probe when the array is small enough to stay cache-resident and the
// space win is large (8KB -> at most ~2KB).
const andNotCompactThreshold = 1024

func AndNot(a, b *Bitmap) *Bitmap {
	res := NewBitmap()
	if a.IsEmpty() {
		return res
	}
	if b.IsEmpty() {
		return a.Clone()
	}

	andNotContainers(a, b, res)
	return res
}

// andNotWalk drives one ordered pass over a's keys, pairing each with b's
// matching container (bc == nil for unmatched keys) and skipping containers
// that are empty by their cardinality header. Both andNotContainers passes use
// it, so the sizing and materialization walks cannot disagree.
func andNotWalk(a, b *Bitmap, visit func(ak uint64, ac, bc []uint16)) {
	an := a.keys.numKeys()
	bn := b.keys.numKeys()
	for ai, bi := 0, 0; ai < an; ai++ {
		ak := a.keys.key(ai)
		for bi < bn && b.keys.key(bi) < ak {
			bi++
		}
		ac := a.getContainer(a.keys.val(ai))
		if getCardinality(ac) == 0 {
			continue
		}
		var bc []uint16
		if bi < bn && b.keys.key(bi) == ak {
			bc = b.getContainer(b.keys.val(bi))
			bi++
		}
		visit(ak, ac, bc)
	}
}

// compactedArraySize is the arena footprint of an array container of
// cardinality n: header, values, one spare slot, padded to a multiple of 4
// uint16s so every later container keeps the 8-byte alignment the
// uint16To64SliceUnsafe views rely on. A full 4096-value array drops the spare
// rather than exceed maxContainerSize.
func compactedArraySize(n int) int {
	return min((int(startIdx)+n+1+3)&^3, maxContainerSize)
}

// andNotResultSize returns the uint16s the result for source ac occupies in
// res at cardinality n. A bitmap source at/above the threshold stays a
// maxContainerSize bitmap, so andNotResultCard's clamp there is harmless.
func andNotResultSize(ac []uint16, n int) int {
	if ac[indexType] == typeBitmap && n >= andNotCompactThreshold {
		return maxContainerSize
	}
	return compactedArraySize(n)
}

func andNotContainers(a, b, res *Bitmap) {
	// Count each pair's result cardinality and total the container sizes, then
	// grow res once. The counts are kept so the materialization below does not
	// recompute them.
	counts := make([]uint16, 0, a.keys.numKeys())
	newKeys, sizeContainers := 0, 0
	andNotWalk(a, b, func(ak uint64, ac, bc []uint16) {
		n := andNotResultCard(ac, bc)
		counts = append(counts, uint16(n))
		if n <= 0 {
			return
		}
		newKeys++
		sizeContainers += andNotResultSize(ac, n)
	})
	if newKeys == 0 {
		return
	}
	res.expandConditionally(newKeys, sizeContainers)

	// Materialize each result: reserve a slot of the counted size and let
	// containerAndNotAlt fill it. Sizing and filling both go through
	// andNotResultSize, so the slot always fits exactly.
	idx := 0
	andNotWalk(a, b, func(ak uint64, ac, bc []uint16) {
		n := int(counts[idx])
		idx++
		if n == 0 {
			return
		}
		sz := andNotResultSize(ac, n)
		offset := res.newContainerNoClr(uint16(sz))
		containerAndNotAlt(ac, bc, res.data[offset:offset+uint64(sz)], 0)
		res.setKey(ak, offset)
	})
}

// AndNot performs in-place AND-NOT of ra and bm (ra &^= bm).
// Neither bitmap has unmatched containers requiring mandatory action:
// unmatched ra containers are kept, unmatched bm containers are irrelevant.
// The smaller bitmap drives for a minor sequential optimisation.
func (ra *Bitmap) AndNot(bm *Bitmap) *Bitmap {
	if bm.IsEmpty() || ra.IsEmpty() {
		return ra
	}

	an, bn := ra.keys.numKeys(), bm.keys.numKeys()
	// Drive from the larger bitmap so the initial binary search hits the
	// smaller side. Only the larger side can trigger galloping (the smaller
	// side can never satisfy candidate > threshold*8), so one flag is always false.
	if bn > an {
		useGallopB := shouldGallop(bn, an)
		andNotContainersInRangeB(ra, bm, 0, bn, nil, false, useGallopB)
	} else {
		useGallopA := shouldGallop(an, bn)
		andNotContainersInRangeA(ra, bm, 0, an, nil, useGallopA, false)
	}

	return ra
}

// AndNotConc performs AndNot merge concurrently (ra &^= bm).
// Neither bitmap has unmatched containers requiring mandatory action, so the
// smaller bitmap drives to minimise goroutine count. Concurrency is calculated
// based on number of internal containers in the smaller bitmap, so that each
// goroutine handles at least [minContainersPerRoutine] containers.
// maxConcurrency limits concurrency calculated internally.
// If maxConcurrency <= 0, then calculated concurrency is not limited.
//
// E.g.: smaller bitmap has 100 containers. Internal concurrency = 100/24 = 4. For:
// - maxConcurrency = 2, there will be 2 goroutines executed
// - maxConcurrency = 6, there will be 4 goroutines executed
func (ra *Bitmap) AndNotConc(bm *Bitmap, maxConcurrency int) *Bitmap {
	if bm.IsEmpty() || ra.IsEmpty() {
		return ra
	}

	// Drive concurrency from the larger bitmap to maximise the number of
	// goroutines. Each goroutine independently handles a disjoint range of
	// the driving bitmap's keys and binary-searches the other bitmap for its
	// starting position — both directions are safe for concurrent execution.
	an, bn := ra.keys.numKeys(), bm.keys.numKeys()
	// Drive from the smaller bitmap: useful work is bounded by min(an, bn)
	// (only matching keys trigger container operations), so goroutines on the
	// larger side would scan with no matches. The larger "other" side is then
	// searched via binary search and is also the one where galloping kicks in.
	n := min(an, bn)
	concurrency := calcConcurrency(n, minContainersPerRoutine, maxConcurrency)

	var andNotCallback func(a, b *Bitmap, i, j int, optBuf []uint16, useGallopAi, useGallopBi bool)
	var useGallopA, useGallopB bool
	if bn < an {
		// B is smaller: drive from B, search A.
		// useGallopB is always false: bn/concurrency < bn < an, so
		// bn/concurrency > an*8 is impossible.
		andNotCallback = andNotContainersInRangeB
		useGallopA = shouldGallop(an, bn/concurrency)
	} else {
		// A is smaller (or equal): drive from A, search B.
		// useGallopA is always false: an/concurrency <= an <= bn, so
		// an/concurrency > bn*8 is impossible.
		andNotCallback = andNotContainersInRangeA
		useGallopB = shouldGallop(bn, an/concurrency)
	}

	callback := func(i, j, _ int) { andNotCallback(ra, bm, i, j, nil, useGallopA, useGallopB) }
	concurrentlyInRanges(n, concurrency, callback)

	return ra
}

func andNotContainersInRangeA(a, b *Bitmap, ai, aj int, optBuf []uint16, useGallopA, useGallopB bool) {
	ak := a.keys.key(ai)
	bi := b.keys.search(ak)
	bn := b.keys.numKeys()
	andNotContainersInRange(a, b, ai, aj, bi, bn, optBuf, useGallopA, useGallopB)
}

func andNotContainersInRangeB(a, b *Bitmap, bi, bj int, optBuf []uint16, useGallopA, useGallopB bool) {
	bk := b.keys.key(bi)
	ai := a.keys.search(bk)
	an := a.keys.numKeys()
	andNotContainersInRange(a, b, ai, an, bi, bj, optBuf, useGallopA, useGallopB)
}

func andNotContainersInRange(a, b *Bitmap, ai, aj, bi, bj int, optBuf []uint16, useGallopA, useGallopB bool) {
	for ai < aj && bi < bj {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			if c := containerAndNotAlt(ac, bc, optBuf, runInline); len(c) > 0 {
				panic("new container not expected in AndNot inline mode")
			}
			ai++
			bi++
		} else if ak < bk {
			if useGallopA {
				ai = a.keys.searchFrom(ai, bk)
			} else {
				ai++
			}
		} else {
			if useGallopB {
				bi = b.keys.searchFrom(bi, ak)
			} else {
				bi++
			}
		}
	}
}

func Or(a, b *Bitmap) *Bitmap {
	res := NewBitmap()
	if ae, be := a.IsEmpty(), b.IsEmpty(); ae && be {
		return res
	} else if ae {
		return b.Clone()
	} else if be {
		return a.Clone()
	}

	buf := make([]uint16, maxContainerSize)
	orContainers(a, b, res, buf)
	return res
}

func orContainers(a, b, res *Bitmap, buf []uint16) {
	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	akToAc := map[uint64][]uint16{}
	bkToBc := map[uint64][]uint16{}
	sizeContainers := 0
	newKeys := 0

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			if c := containerOrAlt(ac, bc, buf, 0); len(c) > 0 && getCardinality(c) > 0 {
				// Since buffer is used in containers merge, result container has to be copied
				// to the bitmap immediately to let buffer be reused in next merge,
				// contrary to unique containers from bitmap a and b copied at the end of method execution

				// create a new container and update the key offset to this container.
				offset := res.newContainerNoClr(uint16(len(c)))
				copy(res.data[offset:], c)
				res.setKey(ak, offset)
			}
			ai++
			bi++
		} else if ak < bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			if getCardinality(ac) > 0 {
				akToAc[ak] = ac
				sizeContainers += len(ac)
				newKeys++
			}
			ai++
		} else {
			off := b.keys.val(bi)
			bc := b.getContainer(off)
			if getCardinality(bc) > 0 {
				bkToBc[bk] = bc
				sizeContainers += len(bc)
				newKeys++
			}
			bi++
		}
	}
	for ; ai < an; ai++ {
		off := a.keys.val(ai)
		ac := a.getContainer(off)
		if getCardinality(ac) > 0 {
			ak := a.keys.key(ai)
			akToAc[ak] = ac
			sizeContainers += len(ac)
			newKeys++
		}
	}
	for ; bi < bn; bi++ {
		off := b.keys.val(bi)
		bc := b.getContainer(off)
		if getCardinality(bc) > 0 {
			bk := b.keys.key(bi)
			bkToBc[bk] = bc
			sizeContainers += len(bc)
			newKeys++
		}
	}

	if sizeContainers > 0 {
		res.expandConditionally(newKeys, sizeContainers)

		for ak, ac := range akToAc {
			// create a new container and update the key offset to this container.
			offset := res.newContainerNoClr(uint16(len(ac)))
			copy(res.data[offset:], ac)
			res.setKey(ak, offset)
		}
		for bk, bc := range bkToBc {
			// create a new container and update the key offset to this container.
			offset := res.newContainerNoClr(uint16(len(bc)))
			copy(res.data[offset:], bc)
			res.setKey(bk, offset)
		}
	}
}

// Or performs in-place OR of ra and bm (ra |= bm).
// bm drives the two-pointer walk: bm containers with no matching ra key must
// be added to ra, so every bm container must be visited. Ra containers with
// no matching bm key are left unchanged — no visit required.
func (ra *Bitmap) Or(bm *Bitmap) *Bitmap {
	if bm.IsEmpty() {
		return ra
	}

	an := ra.keys.numKeys()
	bn := bm.keys.numKeys()
	useGallopA := shouldGallop(an, bn)
	orContainersInRange(ra, bm, 0, bn, useGallopA)
	return ra
}

func orContainersInRange(a, b *Bitmap, bi, bj int, useGallopA bool) {
	buf := make([]uint16, maxContainerSize)

	bk := b.keys.key(bi)
	ai := a.keys.search(bk)
	an := a.keys.numKeys()

	// copy containers from b to a all at once
	// expanding underlying data slice and keys subslice once
	sizeContainers := 0
	newKeys := 0
	initialCap := min(bj-bi, 16)
	bKeys := make([]uint64, 0, initialCap)
	bContainers := make([][]uint16, 0, initialCap)

	for ai < an && bi < bj {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			aoff := a.keys.val(ai)
			ac := a.getContainer(aoff)
			boff := b.keys.val(bi)
			bc := b.getContainer(boff)
			if c := containerOrAlt(ac, bc, buf, runInline); len(c) > 0 {
				// Since buffer is used in containers merge, result container has to be copied
				// to the bitmap immediately to let buffer be reused in next merge,
				// contrary to unique containers from bitmap b copied at the end of method execution

				// Replacing previous container with merged one, that requires moving data
				// to the right to make enough space for merged container is slower
				// than appending bitmap with entirely new container and "forgetting" old one
				// for large bitmaps, so it is performed only on small ones
				if an > 10 {
					// create a new container and update the key offset to this container.
					offset := a.newContainerNoClr(uint16(len(c)))
					copy(a.data[offset:], c)
					a.setKey(ak, offset)
				} else {
					// make room for container, replacing smaller one and update key offset to new container.
					a.insertAt(aoff, c)
					a.setKey(ak, aoff)
				}
			}
			ai++
			bi++
		} else if ak < bk {
			if useGallopA {
				ai = a.keys.searchFrom(ai, bk)
			} else {
				ai++
			}
		} else {
			off := b.keys.val(bi)
			if getCardinality(b.data[off:]) > 0 {
				bc := b.getContainer(off)
				bKeys = append(bKeys, bk)
				bContainers = append(bContainers, bc)
				sizeContainers += len(bc)
				newKeys++
			}
			bi++
		}
	}

	if remaining := bj - bi; remaining > 0 {
		// All remaining b keys are b-only. Pre-extend capacity to avoid
		// append doubling in the tail loop.
		bKeys = slices.Grow(bKeys, remaining)
		bContainers = slices.Grow(bContainers, remaining)
		for ; bi < bj; bi++ {
			off := b.keys.val(bi)
			if getCardinality(b.data[off:]) > 0 {
				bc := b.getContainer(off)
				bk := b.keys.key(bi)
				bKeys = append(bKeys, bk)
				bContainers = append(bContainers, bc)
				sizeContainers += len(bc)
				newKeys++
			}
		}
	}

	if sizeContainers > 0 {
		a.expandConditionally(newKeys, sizeContainers)

		for i, bc := range bContainers {
			// create a new container and update the key offset to this container.
			offset := a.newContainerNoClr(uint16(len(bc)))
			copy(a.data[offset:], bc)
			a.setKey(bKeys[i], offset)
		}
	}
}

// OrConc performs Or merge concurrently (ra |= bm).
// bm drives the walk: every bm container must be visited to add those with no
// matching ra key. Concurrency is calculated based on number of internal
// containers in bm, so that each goroutine handles at least
// [minContainersPerRoutine] containers.
// maxConcurrency limits concurrency calculated internally.
// If maxConcurrency <= 0, then calculated concurrency is not limited.
//
// E.g.: src bitmap has 100 containers. Internal concurrency = 100/24 = 4. For:
// - maxConcurrency = 2, there will be 2 goroutines executed
// - maxConcurrency = 6, there will be 4 goroutines executed
func (ra *Bitmap) OrConc(bm *Bitmap, maxConcurrency int) *Bitmap {
	if bm.IsEmpty() {
		return ra
	}

	an := ra.keys.numKeys()
	bn := bm.keys.numKeys()
	concurrency := calcConcurrency(bn, minContainersPerRoutine, maxConcurrency)
	useGallopA := shouldGallop(an, bn/concurrency)
	if concurrency == 1 {
		orContainersInRange(ra, bm, 0, bn, useGallopA)
		return ra
	}

	var totalNewKeys int
	var totalSizeContainers int
	allKeys := make([][]uint64, concurrency)
	allContainers := make([][][]uint16, concurrency)
	lock := new(sync.Mutex)
	callback := func(bi, bj, i int) {
		newKeys, sizeContainers, keys, containers := orContainersInRangeConc(ra, bm, bi, bj, useGallopA)

		lock.Lock()
		totalNewKeys += newKeys
		totalSizeContainers += sizeContainers
		lock.Unlock()
		allKeys[i] = keys
		allContainers[i] = containers
	}
	concurrentlyInRanges(bn, concurrency, callback)
	if totalSizeContainers > 0 {
		ra.expandConditionally(totalNewKeys, totalSizeContainers)

		for i, containers := range allContainers {
			for j, container := range containers {
				// create a new container and update the key offset to this container.
				offset := ra.newContainerNoClr(uint16(len(container)))
				copy(ra.data[offset:], container)
				ra.setKey(allKeys[i][j], offset)
			}
		}
	}

	return ra
}

func orContainersInRangeConc(a, b *Bitmap, bi, bj int, useGallopA bool,
) (newKeys, sizeContainers int, bKeys []uint64, bContainers [][]uint16) {
	buf := make([]uint16, maxContainerSize)

	bk := b.keys.key(bi)
	ai := a.keys.search(bk)
	an := a.keys.numKeys()

	// copy containers from b to a all at once
	// expanding underlying data slice and keys subslice once
	sizeContainers = 0
	newKeys = 0
	initialCap := min(bj-bi, 16)
	bKeys = make([]uint64, 0, initialCap)
	bContainers = make([][]uint16, 0, initialCap)

	for ai < an && bi < bj {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			c := containerOrAlt(ac, bc, buf, runInline)
			if clen := len(c); clen > 0 {
				cc := make([]uint16, clen)
				copy(cc, c)
				bKeys = append(bKeys, bk)
				bContainers = append(bContainers, cc)
				sizeContainers += clen
			}
			ai++
			bi++
		} else if ak < bk {
			if useGallopA {
				ai = a.keys.searchFrom(ai, bk)
			} else {
				ai++
			}
		} else {
			off := b.keys.val(bi)
			if getCardinality(b.data[off:]) > 0 {
				bc := b.getContainer(off)
				bKeys = append(bKeys, bk)
				bContainers = append(bContainers, bc)
				sizeContainers += len(bc)
				newKeys++
			}
			bi++
		}
	}

	if remaining := bj - bi; remaining > 0 {
		// All remaining b keys are b-only. Pre-extend capacity to avoid
		// append doubling in the tail loop.
		bKeys = slices.Grow(bKeys, remaining)
		bContainers = slices.Grow(bContainers, remaining)
		for ; bi < bj; bi++ {
			off := b.keys.val(bi)
			if getCardinality(b.data[off:]) > 0 {
				bc := b.getContainer(off)
				bk := b.keys.key(bi)
				bKeys = append(bKeys, bk)
				bContainers = append(bContainers, bc)
				sizeContainers += len(bc)
				newKeys++
			}
		}
	}

	return
}

const minContainersPerRoutine = 24

// minKeysForGallop is the minimum number of keys a bitmap must have for
// galloping (exponential+binary search) to outperform sequential bi++ in
// the two-pointer walk. Below this threshold the key node fits comfortably
// in CPU cache and hardware prefetching of sequential access beats the
// scattered memory accesses of exponential search.
// Applies to And/AndConc (galloping on bm) and Or/OrConc (galloping on ra).
const minKeysForGallop = 1000

// shouldGallop reports whether the candidate range is large enough relative to
// the threshold range for exponential search (searchFrom) to outperform linear
// bi++ / ai++. True when candidate is at least 8× the threshold AND candidate
// exceeds minKeysForGallop (below which sequential prefetching wins).
func shouldGallop(candidate, threshold int) bool {
	return candidate > threshold*8 && candidate > minKeysForGallop
}

func calcConcurrency(numContainers, minContainers, maxConcurrency int) int {
	concurrency := max(numContainers/minContainers, 1)
	if maxConcurrency > 0 {
		concurrency = min(concurrency, maxConcurrency)
	}
	return concurrency
}

func concurrentlyInRanges(numContainers, concurrency int, callback func(from, to, i int)) {
	if concurrency <= 1 {
		callback(0, numContainers, 0)
		return
	}

	div := numContainers / concurrency
	mod := numContainers % concurrency

	wg := new(sync.WaitGroup)
	wg.Add(concurrency - 1)

	for i := 0; i < concurrency; i++ {
		i := i
		var from, to int

		if i < mod {
			from = i * (div + 1)
			to = (i + 1) * (div + 1)
		} else {
			from = mod*(div+1) + (i-mod)*div
			to = mod*(div+1) + (i-mod+1)*div
		}

		if i != concurrency-1 {
			go func(f, t, idx int) {
				callback(f, t, idx)
				wg.Done()
			}(from, to, i)
		} else {
			callback(from, to, i)
		}
	}
	wg.Wait()
}

func (ra *Bitmap) ConvertToBitmapContainers() {
	for ai, an := 0, ra.keys.numKeys(); ai < an; ai++ {
		ak := ra.keys.key(ai)
		off := ra.keys.val(ai)
		ac := ra.getContainer(off)

		if ac[indexType] == typeArray {
			c := array(ac).toBitmapContainer(nil)
			offset := ra.newContainer(uint16(len(c)))
			copy(ra.data[offset:], c)
			ra.setKey(ak, offset)
		}
	}
}

func (ra *Bitmap) NumContainers() int {
	if ra == nil {
		return 0
	}
	return ra.keys.numKeys()
}

func (ra *Bitmap) LenInBytes() int {
	if ra == nil {
		return 0
	}
	return len(ra.data) * 2
}

func (ra *Bitmap) capInBytes() int {
	if ra == nil {
		return 0
	}
	return cap(ra.data) * 2
}

func (ra *Bitmap) CloneToBuf(buf []byte) *Bitmap {
	dst := &Bitmap{}
	dst.InitCloneToBuf(ra, buf)
	return dst
}

// InitCloneToBuf makes dst a clone of src stored in buf — the in-place
// equivalent of src.CloneToBuf(buf), reusing dst instead of allocating a new
// Bitmap. Callers that pool result buffers can pool the Bitmap struct along
// with them and re-initialize it on every checkout, so cloning allocates
// nothing when buf is large enough; it panics if buf cannot hold src. dst's
// previous content is discarded, never freed — it must not own anything the
// caller still needs. A nil src initializes dst as an empty bitmap over buf
// (the NewBitmapToBuf semantics); a buf too small for even the empty bitmap
// then falls back to a heap allocation.
func (dst *Bitmap) InitCloneToBuf(src *Bitmap, buf []byte) {
	if src == nil {
		dst.initNewToBuf(buf)
		return
	}

	// Use full capacity, rounded down to an even number of bytes since
	// the bitmap operates on []uint16 (2 bytes per element).
	buf = buf[:cap(buf)/2*2]

	srcLen := src.LenInBytes()
	if srcLen > len(buf) {
		panic(fmt.Sprintf("CloneToBuf: buf too small: need at least %d bytes, got %d", srcLen, cap(buf)))
	}

	// Copy at the uint16 level into the destination buffer, then trim data
	// to the used length while keeping the full buffer capacity available
	// for future growth.
	copy(byteTo16SliceUnsafe(buf), src.data)
	dst.initFromBuffer(buf)
	dst.data = dst.data[:srcLen/2]
}

// FromBufferUnlimited returns a pointer to bitmap corresponding to the given buffer.
// Entire buffer capacity is utlized for future bitmap modifications and expansions.
func FromBufferUnlimited(buf []byte) *Bitmap {
	dst := &Bitmap{}
	dst.InitFromBufferUnlimited(buf)
	return dst
}

// InitFromBufferUnlimited re-points dst at buf — the in-place equivalent of
// FromBufferUnlimited, reusing dst instead of allocating a new Bitmap.
// Callers that pool buffers can pool the Bitmap struct along with them and
// re-initialize it on every checkout, so viewing a buffer allocates nothing;
// only a buf too small for even the empty bitmap (under 8 bytes) falls back
// to a heap allocation. dst's previous content is discarded, never freed —
// it must not own anything the caller still needs.
func (dst *Bitmap) InitFromBufferUnlimited(buf []byte) {
	ln := len(buf)
	assert(ln%2 == 0)
	if len(buf) < 8 {
		*dst = *NewBitmap()
		return
	}

	cp := cap(buf)
	data := buf[:cp]
	if cp%2 != 0 {
		data = buf[:cp-1]
	}

	du := byteTo16SliceUnsafe(data)
	x := uint16To64SliceUnsafe(du[:4])[indexNodeSize]
	*dst = Bitmap{
		data: du[:ln/2],
		_ptr: buf, // Keep a hold of data, otherwise GC would do its thing.
		keys: uint16To64SliceUnsafe(du[:x]),
	}
}

// Prefill creates bitmap prefilled with elements [0-maxX]
func Prefill(maxX uint64) *Bitmap {
	containersCount, remainingCount := calcFullContainersAndRemainingCounts(maxX)
	// create additional container for remaining values
	// (or reserve space for new one if there are not any remaining)
	// +1 additional key to avoid keys expanding (there should always be 1 spare)
	bm := newBitmapWith(int(containersCount)+1+1, maxContainerSize, int(containersCount)*maxContainerSize)
	bm.prefill(containersCount, remainingCount)
	return bm
}

// FillUp fill bitmap with elements (maximum-maxX], where maximum means last element.
// If bitmap is empty then [0-maxX] elements are added
// (reusing underlying data slice if big enough to fit all elements).
// If last element is >= than given maxX nothing is done.
func (ra *Bitmap) FillUp(maxX uint64) {
	if ra == nil {
		return
	}

	maxContainersCount, maxRemainingCount := calcFullContainersAndRemainingCounts(maxX)
	if ra.IsEmpty() {
		// try to fit data into existing memory,
		// if there is not enough space anyway, allocate more memory to fit additional container
		minimalContainersCount := maxContainersCount
		if maxRemainingCount > 0 {
			minimalContainersCount++
		}
		minimalKeysLen := calcInitialKeysLen(minimalContainersCount + 1)
		minimalLen := minimalKeysLen + minimalContainersCount*maxContainerSize

		var bm *Bitmap
		if minimalLen <= cap(ra.data) {
			bm = newBitmapToBuf(minimalKeysLen, maxContainerSize, ra.data)
		} else {
			bm = newBitmapWith(int(maxContainersCount)+1+1, maxContainerSize, int(maxContainersCount)*maxContainerSize)
		}
		bm.prefill(maxContainersCount, maxRemainingCount)
		ra.data = bm.data
		ra._ptr = bm._ptr
		ra.keys = bm.keys
		return
	}

	minX := ra.Maximum()
	if minX >= maxX {
		return
	}

	maxKey := maxX & mask
	minKey := minX & mask
	maxY := int(uint16(maxX))
	minY := int(uint16(minX))

	idx := ra.keys.searchReverse(minKey)
	minOffset := ra.keys.val(idx)

	// same container
	if maxKey == minKey {
		commonContainer := ra.getContainer(minOffset)
		card := getCardinality(commonContainer)
		newYs := maxY - minY

		switch commonContainer[indexType] {
		case typeBitmap:
			// set bits in bitmap
			bitmap(commonContainer).setRange(minY, maxY, nil)
			setCardinality(commonContainer, card+newYs)
		case typeArray:
			size := commonContainer[indexSize]
			if spaceLeft := int(size-startIdx) - card; spaceLeft >= newYs {
				// add elements to existing array
				for i := 0; i < newYs; i++ {
					commonContainer[startIdx+uint16(card+i)] = uint16(minY + 1 + i)
				}
				setCardinality(commonContainer, card+newYs)
			} else {
				// create new bitmap container, copy elements from array, set new bits
				prevContainer := commonContainer
				minOffset = ra.newContainer(maxContainerSize)
				ra.setKey(minKey, minOffset)

				commonContainer = ra.fillUpBitmapContainerRange(minOffset, minY, maxY, card+newYs, nil)
				for i := 0; i < card; i++ {
					y := prevContainer[startIdx+uint16(i)]
					commonContainer[startIdx+y/16] |= bitmapMask[y%16]
				}
			}
		default:
			panic("unknown container type")
		}
		return
	}

	minContainersCount, minRemainingCount := calcFullContainersAndRemainingCounts(minX)
	requiredContainersCount := maxContainersCount - minContainersCount
	if maxRemainingCount > 0 {
		requiredContainersCount++
	}

	// first count how many new containers will be required to allocate memory once, then do the fillup
	var fillUpCommonContainer func(commonContainer []uint16, onesBitmap bitmap) = nil
	// idx of first full container to be added
	containerIdx := minContainersCount
	if minRemainingCount > 0 {
		containerIdx++
		requiredContainersCount--

		commonContainer := ra.getContainer(minOffset)
		card := getCardinality(commonContainer)
		newYs := maxCardinality - 1 - minY

		switch commonContainer[indexType] {
		case typeBitmap:
			// if bitmap, set proper bits up to maxCardinality
			fillUpCommonContainer = func(commonContainer []uint16, onesBitmap bitmap) {
				bitmap(commonContainer).setRange(minY, maxCardinality-1, onesBitmap)
				setCardinality(commonContainer, card+newYs)
			}
		case typeArray:
			size := commonContainer[indexSize]
			if spaceLeft := int(size-startIdx) - card; spaceLeft >= newYs {
				// if array add new elements if there is enough space left
				fillUpCommonContainer = func(commonContainer []uint16, onesBitmap bitmap) {
					for i := 0; i < newYs; i++ {
						commonContainer[startIdx+uint16(card+i)] = uint16(minY + 1 + i)
					}
					setCardinality(commonContainer, card+newYs)
				}
			} else {
				// if not enough space, create new bitmap container, set new bits and set old ones
				requiredContainersCount++
				fillUpCommonContainer = func(commonContainer []uint16, onesBitmap bitmap) {
					prevContainer := commonContainer
					offset := ra.newContainer(maxContainerSize)
					ra.setKey(minKey, offset)

					commonContainer = ra.fillUpBitmapContainerRange(offset, minY, maxCardinality-1, card+newYs, onesBitmap)
					for i := 0; i < card; i++ {
						y := prevContainer[startIdx+uint16(i)]
						commonContainer[startIdx+y/16] |= bitmapMask[y%16]
					}
				}
			}
		default:
			panic("unknown container type")
		}
	}

	// calculate required memory to allocate and expand underlying slice
	ra.expandConditionally(requiredContainersCount, requiredContainersCount*maxContainerSize)

	var onesBitmap bitmap
	if containerIdx < maxContainersCount {
		// fillup full containers
		key := uint64(containerIdx*maxCardinality) & mask
		offset := ra.newContainerNoClr(maxContainerSize)
		ra.setKey(key, offset)

		onesBitmap = ra.fillUpBitmapContainers(offset, containerIdx+1, maxContainersCount)
	}
	if maxRemainingCount > 0 {
		// fillup last (highest) container
		key := uint64(maxContainersCount*maxCardinality) & mask
		offset := ra.newContainer(maxContainerSize)
		ra.setKey(key, offset)

		ra.fillUpBitmapContainerRange(offset, 0, maxRemainingCount-1, maxRemainingCount, onesBitmap)
	}
	if minRemainingCount > 0 {
		// fillup common container using previously created callback.
		// due to slice expanding, container has to be fetched once using new offset
		minOffset = ra.keys.val(idx)
		commonContainer := ra.getContainer(minOffset)
		fillUpCommonContainer(commonContainer, onesBitmap)
	}
}

// prefill prefills containersCount full containers
// and last one with remainingCount first values
func (ra *Bitmap) prefill(containersCount, remainingCount int) {
	var onesBitmap bitmap
	if containersCount > 0 {
		offset := ra.keys.val(0)
		onesBitmap = ra.fillUpBitmapContainers(offset, 1, containersCount)
	}
	if remainingCount > 0 {
		var offset uint64
		if containersCount > 0 {
			// create container for remaining values
			key := uint64(containersCount*maxCardinality) & mask
			offset = ra.newContainer(maxContainerSize)
			ra.setKey(key, offset)
		} else {
			// get initial container
			offset = ra.keys.val(0)
		}
		ra.fillUpBitmapContainerRange(offset, 0, remainingCount-1, remainingCount, onesBitmap)
	}
}

// fillUpBitmapContainerRange gets container by offset, sets its type to bitmap,
// sets bits in range minY-maxY (both included)
func (ra *Bitmap) fillUpBitmapContainerRange(offset uint64, minY, maxY, card int, onesBitmap bitmap) bitmap {
	b := bitmap(ra.getContainer(offset))
	b[indexSize] = maxContainerSize
	b[indexType] = typeBitmap
	setCardinality(b, card)

	b.setRange(minY, maxY, onesBitmap)
	return b
}

// fillUpBitmapContainers gets container by offset, sets its type to bitmap,
// sets all bits,
// then creates [minIdx-maxIdx) containers with all bits set, by copying first container
func (ra *Bitmap) fillUpBitmapContainers(offset uint64, minIdx, maxIdx int) bitmap {
	ones := bitmap(ra.getContainer(offset)[:maxContainerSize])
	ones[indexSize] = maxContainerSize
	ones[indexType] = typeBitmap
	setCardinality(ones, maxCardinality)

	// fill entire bitmap container with ones
	ones.fillWithOnes()

	// fill remaining containers by copying first one
	for i := minIdx; i < maxIdx; i++ {
		key := uint64(i*maxCardinality) & mask
		offset := ra.newContainerNoClr(maxContainerSize)
		ra.setKey(key, offset)
		copy(ra.data[offset:], ones)
	}
	return ones
}

func calcFullContainersAndRemainingCounts(maxX uint64) (int, int) {
	maxCard64 := uint64(maxCardinality)

	// maxX should be included, therefore +1
	containers := maxX / maxCard64
	remaining := maxX % maxCard64
	if remaining == maxCard64-1 {
		containers++
	}
	remaining = (remaining + 1) % maxCard64
	return int(containers), int(remaining)
}

func (b bitmap) setRange(minY, maxY int, onesBitmap bitmap) {
	minY16 := (minY + 15) / 16 * 16
	maxY16 := (maxY + 1) / 16 * 16

	// fmt.Printf("  ==> minY [%d] minY16 [%d] minY64 [%d]\n", minY, minY16, (minY+63)/64*64)
	// fmt.Printf("  ==> maxY [%d] maxY16 [%d] maxY64 [%d]\n", maxY, maxY16, (maxY+1)/64*64)

	b16 := b[startIdx:]
	if onesBitmap != nil {
		if mn, mx := uint16(minY16/16), uint16(maxY16/16); mn < mx {
			copy(b16[mn:mx], onesBitmap[startIdx+mn:startIdx+mx])
		}
	} else {
		minY64 := (minY + 63) / 64 * 64
		maxY64 := (maxY + 1) / 64 * 64

		if mn, mx := minY64/64, maxY64/64; mn < mx {
			b64 := uint16To64SliceUnsafe(b16)
			for i := mn; i < mx; i++ {
				// fmt.Printf("    ==> b64 i=%d\n", i)
				b64[i] = math.MaxUint64
			}
		}
		for i, mx := minY16/16, min(minY64/16, maxY16/16); i < mx; i++ {
			// fmt.Printf("    ==> b64L i=%d\n", i)
			b16[i] = math.MaxUint16
		}
		for i, mx := max(minY16/16, maxY64/16), maxY16/16; i < mx; i++ {
			// fmt.Printf("    ==> b64R i=%d\n", i)
			b16[i] = math.MaxUint16
		}
	}
	for y, mx := minY, min(minY16, maxY+1); y < mx; y++ {
		// fmt.Printf("    ==> b16L i=%d bit=%d\n", y/16, y%16)
		b16[y/16] |= bitmapMask[y%16]
	}
	for y, mx := max(minY, maxY16), maxY+1; y < mx; y++ {
		// fmt.Printf("    ==> b16R i=%d bit=%d\n", y/16, y%16)
		b16[y/16] |= bitmapMask[y%16]
	}
}

func (b bitmap) fillWithOnes() {
	b64 := uint16To64SliceUnsafe(b[startIdx:])
	for i := range b64 {
		b64[i] = math.MaxUint64
	}
}

func (ra *Bitmap) expandConditionally(newKeys int, sizeContainers int) {
	cp := cap(ra.data)
	ln := len(ra.data)
	curNumKeys := ra.keys.numKeys()
	curSizeKeys := ra.keys.size()

	sizeKeys := 8 * newKeys // 2x uint64 (key+offset) = 8x uint16
	if curNumKeys+newKeys < ra.keys.maxKeys() {
		if ln+sizeContainers <= cp {
			// keys and containers fit. nothing to do
			return
		}
		// keys fit, containers do not. expand slice to make room for containers and only new keys
	} else {
		if ln+sizeKeys+sizeContainers <= cp {
			// keys do not fit, containers do. just move containers to make room for keys
			if curNumKeys > newKeys {
				// make room for up to curNumKeys additional keys
				sizeKeys = 8 * curNumKeys // 2x uint64 (key+offset) = 8x uint16
				if left := cp - ln - sizeContainers; left < sizeKeys {
					sizeKeys = left / 8 * 8
				}
			}

			// new containers will fit. just move containers to make room for keys
			newSizeKeys := curSizeKeys + sizeKeys

			ra.data = ra.data[:ln+sizeKeys]
			n := copy(ra.data[newSizeKeys:], ra.data[curSizeKeys:])
			ra.memMoved += n
			clear(ra.data[curSizeKeys:newSizeKeys]) // Zero out the space in the middle.

			ra.keys = uint16To64SliceUnsafe(ra.data[:newSizeKeys])
			ra.keys.setNodeSize(newSizeKeys)
			ra.keys.addToAllVals(uint64(sizeKeys))
			return
		}
		// neither keys nor containers fit. expand slice to make room for containers and more keys
		sizeKeys = 8 * max(newKeys, curNumKeys) // 2x uint64 (key+offset) = 8x uint16
	}

	// expand 2x (or up to sizeKeys+sizeNewContainers if 2x is too little)
	growBy := max(cp, sizeKeys+sizeContainers)
	out := make([]uint16, ln+sizeKeys, cp+growBy)

	newSizeKeys := curSizeKeys + sizeKeys
	copy(out, ra.data[:curSizeKeys])
	copy(out[newSizeKeys:], ra.data[curSizeKeys:])
	ra.data = out
	ra._ptr = nil // Allow Go to GC whatever this was pointing to.
	// Re-reference ra.keys correctly because underlying array has changed.

	ra.keys = uint16To64SliceUnsafe(ra.data[:newSizeKeys])
	ra.keys.setNodeSize(newSizeKeys)
	ra.keys.addToAllVals(uint64(sizeKeys))
}

// Masked applies the given mask to every key and returns a new bitmap.
// Keys that collapse to the same masked value have their containers merged
// via container-level OR operations.
func (ra *Bitmap) Masked(mask uint64) *Bitmap {
	return ra.maskedInto(mask, NewBitmap())
}

// MaskedToBuf is like Masked but uses the provided byte slice as the
// underlying buffer for the result bitmap, avoiding heap allocation when
// the buffer is large enough.
func (ra *Bitmap) MaskedToBuf(mask uint64, buf []byte) *Bitmap {
	return ra.maskedInto(mask, NewBitmapToBuf(buf))
}

// writeToMaskedResult writes src into res under maskedKey, OR-merging with
// any existing container for that key. orBuf is a scratch buffer for the OR
// operation when inline merge fails (container grew beyond current allocation).
func writeToMaskedResult(res *Bitmap, maskedKey uint64, src, orBuf []uint16) {
	off, has := res.keys.getValue(maskedKey)
	if !has {
		// First container for this masked key — copy it directly.
		res.expandConditionally(0, len(src))
		off = res.newContainerNoClr(uint16(len(src)))
		copy(res.data[off:], src)
		res.setKey(maskedKey, off)
	} else {
		// Merge with the existing container via OR.
		existing := res.getContainer(off)
		if c := containerOrAlt(existing, src, orBuf, runInline); len(c) > 0 {
			// Inline failed (container grew). If the old container
			// is at the end of res.data, trim and regrow in place to
			// avoid dead space. Otherwise append (dead space).
			if off+uint64(len(existing)) == uint64(len(res.data)) {
				res.data = res.data[:off]
			}
			res.expandConditionally(0, len(c))
			off = res.newContainerNoClr(uint16(len(c)))
			copy(res.data[off:], c)
			res.setKey(maskedKey, off)
		}
	}
}

// maskedInto is the core implementation of Masked and MaskedToBuf.
//
// Alternative approach was considered and benchmarked: sort source keys by
// masked value upfront, count distinct masked keys for exact key pre-allocation,
// accumulate OR per group in scratch buffers, then write each group's final
// container once — eliminating getValue binary searches, setKey shifts, and
// dead-space risk. In practice this was not faster: the extra allocations
// (maskedEntry slice + two scratch buffers vs one) and the sort cost outweigh
// the savings from avoiding binary searches and in-place OR. Memory consumption
// was also higher in most cases. Current approach wins.
func (ra *Bitmap) maskedInto(mask uint64, b *Bitmap) *Bitmap {
	if ra == nil {
		return b
	}
	an := ra.keys.numKeys()
	if an == 0 {
		return b
	}

	// Ensure the mask has its lowest 16 bits unset, since keys always do.
	mask &= 0xFFFFFFFFFFFF0000

	// Pre-size key space so that expandConditionally calls inside the loop
	// don't need to repeatedly move container data to make room for keys.
	b.expandConditionally(an, 0)

	buf := make([]uint16, maxContainerSize)
	for ai := 0; ai < an; ai++ {
		ak := ra.keys.key(ai)
		aoff := ra.keys.val(ai)
		ac := ra.getContainer(aoff)

		if getCardinality(ac) == 0 {
			continue
		}

		maskedKey := ak & mask
		writeToMaskedResult(b, maskedKey, ac, buf)
	}
	return b
}

// MaskedAnd returns a new bitmap containing the AND of a and b, with mask
// applied to all keys. Keys that become equal after masking have their
// containers merged via OR. This is equivalent to Masked(And(a, b), mask)
// but allocates only one bitmap instead of two. Neither a nor b is modified.
// The lowest 16 bits of keys are always zeroed.
func MaskedAnd(a, b *Bitmap, mask uint64) *Bitmap {
	return maskedAndInto(a, b, mask, NewBitmap())
}

// MaskedAndToBuf is like MaskedAnd but uses the provided byte slice as the
// underlying buffer for the result bitmap, avoiding heap allocation when
// the buffer is large enough. The lowest 16 bits of keys are always zeroed.
func MaskedAndToBuf(a, b *Bitmap, mask uint64, buf []byte) *Bitmap {
	return maskedAndInto(a, b, mask, NewBitmapToBuf(buf))
}

func maskedAndInto(a, b *Bitmap, mask uint64, res *Bitmap) *Bitmap {
	if a.IsEmpty() || b.IsEmpty() {
		return res
	}

	mask &= 0xFFFFFFFFFFFF0000

	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	// AND can produce at most min(an, bn) distinct keys before masking.
	minKeys := min(an, bn)
	res.expandConditionally(minKeys, 0)

	// Only the larger side can trigger galloping — the smaller side can never
	// satisfy candidate > threshold*8, so exactly one flag can be true.
	useGallopA := shouldGallop(an, bn)
	useGallopB := shouldGallop(bn, an)

	// andBuf holds the AND result for one container pair.
	// orBuf is the scratch for merging into res when masked keys collide.
	// They must be separate since andBuf is the OR source.
	andBuf := make([]uint16, maxContainerSize)
	orBuf := make([]uint16, maxContainerSize)

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)

		if ak == bk {
			ac := a.getContainer(a.keys.val(ai))
			bc := b.getContainer(b.keys.val(bi))

			c := containerAndAlt(ac, bc, andBuf, 0)
			if len(c) > 0 && getCardinality(c) > 0 {
				maskedKey := ak & mask
				writeToMaskedResult(res, maskedKey, c, orBuf)
			}
			ai++
			bi++
		} else if ak < bk {
			if useGallopA {
				ai = a.keys.searchFrom(ai, bk)
			} else {
				ai++
			}
		} else {
			if useGallopB {
				bi = b.keys.searchFrom(bi, ak)
			} else {
				bi++
			}
		}
	}
	return res
}

// maskedEntry pairs a bitmap container's offset with the masked value of its key.
type maskedEntry struct{ maskedKey, offset uint64 }

// buildMaskedEntries returns a sorted slice of maskedEntry for bm, with each
// key masked by mask. The result is sorted by maskedKey for binary search.
func buildMaskedEntries(bm *Bitmap, mask uint64) []maskedEntry {
	n := bm.keys.numKeys()
	entries := make([]maskedEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = maskedEntry{
			maskedKey: bm.keys.key(i) & mask,
			offset:    bm.keys.val(i),
		}
	}
	slices.SortFunc(entries, func(a, b maskedEntry) int {
		if a.maskedKey < b.maskedKey {
			return -1
		}
		if a.maskedKey > b.maskedKey {
			return 1
		}
		return 0
	})
	return entries
}

// maskedEntriesFor returns the subslice of entries whose maskedKey equals key
// and the position to pass as from on the next call, or nil if no match exists.
// from is a hint: the caller passes back the value returned by the previous
// call, allowing a linear scan across a sorted sequence of keys instead of a
// binary search on every call.
// entries must be sorted by maskedKey, and successive calls must use
// non-decreasing key values.
func maskedEntriesFor(entries []maskedEntry, key uint64, from int) ([]maskedEntry, int) {
	for from < len(entries) && entries[from].maskedKey < key {
		from++
	}
	if from >= len(entries) || entries[from].maskedKey != key {
		return nil, from
	}
	lo := from
	hi := lo + 1
	for hi < len(entries) && entries[hi].maskedKey == key {
		hi++
	}
	return entries[lo:hi], hi
}

// AndMasked is equivalent to ra.And(b.Masked(mask)) but avoids allocating
// an intermediate masked bitmap. b is not modified.
//
// For each key k in ra, AndMasked finds all keys k' in b where k'&mask == k,
// ORs their containers together, then ANDs the result into ra's container at k.
// If no key in b maps to k under the mask, ra's container at k is zeroed out.
// The lowest 16 bits of the mask are always ignored.
func (ra *Bitmap) AndMasked(b *Bitmap, mask uint64) *Bitmap {
	if b.IsEmpty() {
		ra.ZeroOut()
		return ra
	}

	mask &= 0xFFFFFFFFFFFF0000
	entries := buildMaskedEntries(b, mask)
	andMaskedContainersInRange(ra, b, entries, 0, ra.keys.numKeys())
	return ra
}

// AndMaskedConc is like AndMasked but processes ra's containers concurrently.
// Concurrency is calculated based on number of internal containers in ra, so
// that each goroutine handles at least [minContainersPerRoutine] containers.
// maxConcurrency limits concurrency calculated internally.
// If maxConcurrency <= 0, then calculated concurrency is not limited.
func (ra *Bitmap) AndMaskedConc(b *Bitmap, mask uint64, maxConcurrency int) *Bitmap {
	if b.IsEmpty() {
		ra.ZeroOut()
		return ra
	}

	mask &= 0xFFFFFFFFFFFF0000

	// Build entries once; goroutines only read from it.
	entries := buildMaskedEntries(b, mask)

	an := ra.keys.numKeys()
	concurrency := calcConcurrency(an, minContainersPerRoutine, maxConcurrency)
	callback := func(ai, aj, _ int) { andMaskedContainersInRange(ra, b, entries, ai, aj) }
	concurrentlyInRanges(an, concurrency, callback)
	return ra
}

// CopresenceByMask returns a new bitmap containing every value v in the
// union of bms whose masked value (v & mask) is present in bms[i].Masked(mask)
// for every i. Original (unmasked) values are preserved. Equivalent in
// result to the (much more expensive) fold:
//
//	out := bms[0].Masked(mask)
//	for i := 1; i < len(bms); i++ {
//	    out = out.And(bms[i].Masked(mask))
//	}
//	// then map each masked value back to every original v in any bms[i]
//	// whose v & mask matches.
//
// but processes all inputs in one pass — avoiding intermediate bitmap
// allocations and walking each input only once. None of the bms are
// modified.
//
// Mask shape requirement: mask & 0xFFFF == 0xFFFF. Other mask shapes are
// not supported and will yield incorrect results.
//
// Edge cases:
//   - len(bms) == 0: empty result.
//   - len(bms) == 1: result is a clone of bms[0] (single-input co-presence
//     is trivially the input).
//   - any input empty: empty result.
//
// Algorithm: with mask_high = mask & 0xFFFFFFFFFFFF0000, group each input's
// containers by K & mask_high. Walk the masked-key entry slices of all N
// inputs in lockstep (multi-pointer max-key advance) to find groups present
// on every side. For each such group:
//  1. compute pos_i = OR of positions across input i's containers in the group
//  2. common_pos = AND_i (pos_i) — positions co-present in every side
//  3. for each unique original key K appearing in any input's group, emit
//     OR_i(bms[i][K]) AND common_pos at K (omitting empty results)
//
// Step 2 short-circuits as soon as common_pos becomes empty.
func CopresenceByMask(bms []*Bitmap, mask uint64) *Bitmap {
	if len(bms) == 0 {
		return NewBitmap()
	}
	if len(bms) == 1 {
		return bms[0].Clone()
	}
	return copresenceByMaskInto(bms, mask, NewBitmap())
}

// CopresenceByMaskToBuf is like [CopresenceByMask] but uses the provided
// byte slice as the underlying buffer for the result bitmap, avoiding the
// result's heap allocation when the buffer is large enough. If the buffer
// is too small the bitmap grows internally (and allocates), so correctness
// is unaffected — only the allocation-elision is.
//
// Sizing guidance: a safe upper bound is the sum of input byte sizes
// (sum_i bms[i].LenInBytes()). The result's keys area is bounded by the
// sum of input keys areas, and each result container is bounded by the
// sum of its contributing inputs' containers (the per-key OR can't exceed
// it; the subsequent AND with commonPos can only shrink). In practice
// results are often much smaller — callers that know their workload's
// typical density can pass a smaller buffer and accept occasional growth.
//
// Same mask shape requirement as [CopresenceByMask]: mask & 0xFFFF == 0xFFFF.
func CopresenceByMaskToBuf(bms []*Bitmap, mask uint64, buf []byte) *Bitmap {
	if len(bms) == 0 {
		return NewBitmapToBuf(buf)
	}
	if len(bms) == 1 {
		return bms[0].CloneToBuf(buf)
	}
	return copresenceByMaskInto(bms, mask, NewBitmapToBuf(buf))
}

// copresenceByMaskInto is the shared body of [CopresenceByMask] and
// [CopresenceByMaskToBuf]. Caller must ensure len(bms) >= 2; the N=0 and
// N=1 short-circuits are handled at the public entry points because they
// differ in how `res` is constructed (NewBitmap vs Clone[ToBuf]).
func copresenceByMaskInto(bms []*Bitmap, mask uint64, res *Bitmap) *Bitmap {
	n := len(bms)
	for _, bm := range bms {
		if bm.IsEmpty() {
			return res
		}
	}

	maskHigh := mask & 0xFFFFFFFFFFFF0000

	allEntries := make([][]keyedMaskedEntry, n)
	totalEntries := 0
	for i, bm := range bms {
		allEntries[i] = buildKeyedMaskedEntries(bm, maskHigh)
		if len(allEntries[i]) == 0 {
			return res
		}
		totalEntries += len(allEntries[i])
	}

	// Pre-size res's keys area for an estimated result-key count to avoid
	// expandKeys-driven shifts during emission. The result has at most one
	// emitted key per non-empty container across all inputs (sum of
	// totalEntries), so sum/N (the average per input) is a middle-ground
	// estimate: under-estimates only when distinct keys-per-masked-group
	// exceed the per-input average. Under-allocation is recoverable
	// (expandKeys still fires); the goal is to skip it in the common case.
	res.expandConditionally(totalEntries/n, 0)

	// Three scratch buffers carry the full algorithm:
	//   - posBufA / posFallbackA back the OR over the seed group. The result
	//     becomes commonPos (mutated via AND-in-place across the AND-fold);
	//     the unused half is returned as `spare` and reused below.
	//   - posBufB is the third buffer. Paired with `spare`, it backs the
	//     OR over each non-seed group during the AND-fold.
	// Once the AND-fold completes, `spare` and `posBufB` are free (their
	// last contents are the last iPos's OR-result, already folded into
	// commonPos). Emit reuses them as its mergeBuf and mergeFallback —
	// emit needs no third buffer because its final AND target is also
	// mergeFallback (mergeBuf/mergeFallback always point to different
	// underlying buffers, so writing to mergeFallback never aliases merged).
	posBufA := make([]uint16, maxContainerSize)
	posFallbackA := make([]uint16, maxContainerSize)
	posBufB := make([]uint16, maxContainerSize)

	entryCursors := make([]int, n)
	emitCursors := make([]int, n)
	groups := make([][]keyedMaskedEntry, n)
	// foldOrder[k] is the index of the input to fold at position k of the
	// AND-fold. Sorted by ascending estimated |pos_i| so the smallest pos_i
	// seeds commonPos — minimizing intermediate sizes and maximizing the
	// chance of early break-on-empty. cardEst[i] holds the cardinality
	// upper bound (sum of container cardinalities) for input i in the
	// current masked group. Both reused across iterations.
	foldOrder := make([]int, n)
	cardEst := make([]int, n)

	for nextCommonGroup(allEntries, entryCursors, groups) {
		buildFoldOrder(bms, groups, foldOrder, cardEst)

		// commonPos lives in a buffer we mutate via AND-in-place; spare
		// is the other of posBufA/posFallbackA, reused as one of the two
		// buffers for every subsequent iPos OR (and again for emit).
		seed := foldOrder[0]
		commonPos, spare := orPositionsInGroup(bms[seed], groups[seed], posBufA, posFallbackA)
		for k := 1; k < n; k++ {
			i := foldOrder[k]
			iPos := orPositionsInGroupReadOnly(bms[i], groups[i], spare, posBufB)
			if c := containerAndAlt(commonPos, iPos, nil, runInline); len(c) > 0 {
				panic("CopresenceByMask: AND inline returned a new container")
			}
			if getCardinality(commonPos) == 0 {
				break
			}
		}
		if getCardinality(commonPos) == 0 {
			continue
		}

		clear(emitCursors)
		// spare and posBufB held the last iPos's data, which has already
		// been AND-folded into commonPos. Free to reuse as emit's merge
		// pair.
		emitGroupFilteredN(bms, groups, emitCursors, commonPos, spare, posBufB, res)
	}
	return res
}

// buildFoldOrder writes input indices into foldOrder, sorted ascending by
// estimated |pos_i| (the OR of positions across input i's containers in
// the current masked group). The estimate is the sum of those containers'
// cardinalities — exact for single-container groups, an upper bound when
// containers' positions can overlap.
//
// Sorting smallest-first seeds the AND-fold with the smallest pos_i,
// minimizing intermediate sizes and maximizing the chance of early
// break-on-empty.
//
// cardEst is caller-provided scratch the same length as foldOrder; its
// contents are overwritten on each call.
//
// Small-N fast paths matter: this is called once per masked-group
// iteration on the hot path. slices.SortFunc has enough per-call overhead
// that it would dominate per-iteration cost for N=2 (the most common case
// via the binary entry point).
func buildFoldOrder(
	bms []*Bitmap, groups [][]keyedMaskedEntry,
	foldOrder, cardEst []int,
) {
	n := len(groups)
	for i := 0; i < n; i++ {
		sum := 0
		for _, e := range groups[i] {
			sum += getCardinality(bms[i].data[e.offset:])
		}
		cardEst[i] = sum
		foldOrder[i] = i
	}

	switch n {
	case 0, 1:
		return
	case 2:
		// foldOrder is [0, 1] (identity); swap if cardEst[1] < cardEst[0].
		if cardEst[1] < cardEst[0] {
			foldOrder[0], foldOrder[1] = 1, 0
		}
	default:
		slices.SortFunc(foldOrder, func(a, b int) int {
			return cardEst[a] - cardEst[b]
		})
	}
}

// nextCommonGroup advances entryCursors to the next masked-key value that
// is present in all inputs, then writes the per-input slice of entries
// sharing that key into groups. Returns false when no further common key
// exists (i.e. at least one cursor has been exhausted).
//
// Both entryCursors and groups are mutated in place so the caller can reuse
// them across iterations without per-call allocation. groups must already
// have length len(allEntries); its slice headers are overwritten.
//
// Algorithm: multi-pointer max-key walk. At each step, find the largest
// masked-key value under any cursor and gallop every cursor below it
// forward to the first entry with maskedKey >= that target. Repeat until
// either all cursors agree on the same key (a common group is found) or
// some cursor runs off the end (no more common groups possible). Galloping
// is repeated rather than done in a single pass because cursors that
// advance past their previous key may land on a value higher than the
// previous max — forcing a new round of alignment.
func nextCommonGroup(
	allEntries [][]keyedMaskedEntry, entryCursors []int, groups [][]keyedMaskedEntry,
) bool {
	n := len(allEntries)
	for {
		// Exhaustion check: any cursor at end → no more common groups possible.
		for i := 0; i < n; i++ {
			if entryCursors[i] >= len(allEntries[i]) {
				return false
			}
		}

		// Find max masked-key under current cursors.
		maxKey := allEntries[0][entryCursors[0]].maskedKey
		for i := 1; i < n; i++ {
			if k := allEntries[i][entryCursors[i]].maskedKey; k > maxKey {
				maxKey = k
			}
		}

		// Gallop any cursor below maxKey forward to the first entry >= maxKey.
		advanced := false
		for i := 0; i < n; i++ {
			if entryCursors[i] < len(allEntries[i]) && allEntries[i][entryCursors[i]].maskedKey < maxKey {
				entryCursors[i] = searchKeyedMaskedFrom(allEntries[i], entryCursors[i], maxKey)
				advanced = true
			}
		}
		if advanced {
			continue
		}

		// All cursors agree on maxKey: collect each input's group of entries
		// sharing that masked key, advance cursors past them, return.
		for i := 0; i < n; i++ {
			start := entryCursors[i]
			for entryCursors[i] < len(allEntries[i]) && allEntries[i][entryCursors[i]].maskedKey == maxKey {
				entryCursors[i]++
			}
			groups[i] = allEntries[i][start:entryCursors[i]]
		}
		return true
	}
}

// keyedMaskedEntry pairs a container's offset and original key with the
// masked-key value used for grouping. Sorted slices of these support
// two-pointer walks over masked-key groups while preserving access to the
// original (unmasked) key for emission.
type keyedMaskedEntry struct {
	maskedKey   uint64
	originalKey uint64
	offset      uint64
}

// buildKeyedMaskedEntries returns a slice of entries for bm, one per
// non-empty container, sorted by (maskedKey, originalKey). Sorting by
// originalKey within a masked group lets the per-group emission walk both
// sides in parallel by original key.
func buildKeyedMaskedEntries(bm *Bitmap, mask uint64) []keyedMaskedEntry {
	n := bm.keys.numKeys()
	entries := make([]keyedMaskedEntry, 0, n)
	for i := 0; i < n; i++ {
		offset := bm.keys.val(i)
		if getCardinality(bm.data[offset:]) == 0 {
			continue
		}
		key := bm.keys.key(i)
		entries = append(entries, keyedMaskedEntry{
			maskedKey:   key & mask,
			originalKey: key,
			offset:      offset,
		})
	}
	slices.SortFunc(entries, func(a, b keyedMaskedEntry) int {
		if a.maskedKey != b.maskedKey {
			if a.maskedKey < b.maskedKey {
				return -1
			}
			return 1
		}
		if a.originalKey < b.originalKey {
			return -1
		}
		if a.originalKey > b.originalKey {
			return 1
		}
		return 0
	})
	return entries
}

// searchKeyedMaskedFrom returns the index of the smallest entry whose
// maskedKey >= target, starting the search at from+1. The caller must
// guarantee entries[from].maskedKey < target (i.e. that an advance is
// actually needed). Returns len(entries) if no such entry exists.
//
// Uses exponential search to bracket the target then binary search to
// pinpoint it — O(log gap) where gap is the distance from `from` to the
// result. Mirrors keys.searchFrom for []keyedMaskedEntry. Equivalent in
// outcome to a linear `for cursor++` advance but logarithmically faster
// when one input has a long stretch of masked keys absent from another.
func searchKeyedMaskedFrom(entries []keyedMaskedEntry, from int, target uint64) int {
	N := len(entries)
	lower := from + 1
	if lower >= N || entries[lower].maskedKey >= target {
		return lower
	}
	// Exponential expansion to bracket target.
	span := 1
	for lower+span < N && entries[lower+span].maskedKey < target {
		span *= 2
	}
	upper := lower + span
	if upper >= N {
		upper = N - 1
	}
	if entries[upper].maskedKey < target {
		return N
	}
	// Binary search within [lower + span/2, upper].
	lower += span >> 1
	for lower+1 < upper {
		mid := (lower + upper) >> 1
		k := entries[mid].maskedKey
		if k < target {
			lower = mid
		} else if k > target {
			upper = mid
		} else {
			return mid
		}
	}
	return upper
}

// orPositionsInGroupReadOnly is the no-copy variant of orPositionsInGroup
// for callers that will treat the result as read-only. For a single-container
// group it returns the source container slice directly (no buffer write);
// otherwise it falls through to orPositionsInGroup. The returned slice MUST
// NOT be mutated by the caller — use only as an input to operations that
// do not modify their arguments (e.g. AND into a separate accumulator
// buffer, never AND-in-place into this slice). The spare return from the
// underlying orPositionsInGroup is dropped since this variant's callers
// don't track it.
func orPositionsInGroupReadOnly(bm *Bitmap, group []keyedMaskedEntry, bufA, bufB []uint16) []uint16 {
	if len(group) == 1 {
		return bm.getContainer(group[0].offset)
	}
	result, _ := orPositionsInGroup(bm, group, bufA, bufB)
	return result
}

// orPositionsInGroup returns a slice holding the OR of all containers
// referenced by group (using bm for data). bufA and bufB are scratch.
//
// Returns (result, spare):
//   - result points to the buffer holding the OR.
//   - spare points to the OTHER of {bufA, bufB} and is available to the
//     caller for any unrelated use — its contents are stale OR-intermediate
//     data but no live references remain.
//
// The first container is copied into bufA so the source containers are
// never mutated; subsequent ORs use the runInline + fallback swap pattern.
func orPositionsInGroup(bm *Bitmap, group []keyedMaskedEntry, bufA, bufB []uint16) (result, spare []uint16) {
	first := bm.getContainer(group[0].offset)
	copy(bufA, first)
	result, spare = bufA, bufB

	for i := 1; i < len(group); i++ {
		c := containerOrAlt(result, bm.getContainer(group[i].offset), spare, runInline)
		if len(c) > 0 {
			// Inline failed (array→bitmap conversion): result is now in spare.
			result, spare = spare, result
		}
	}
	return result, spare
}

// emitGroupFilteredN walks all N groups in lockstep by originalKey. For
// each unique original key K appearing in any of the groups, it emits
// OR_i(bms[i][K]) AND commonPos at K in res when non-empty.
//
// Buffer protocol for the per-key OR over up to N source containers:
//   - First source contributing to K: `merged` aliases the source slice
//     directly (no copy).
//   - Second source: OR via mode-0 into mergeBuf, `merged` now points to
//     mergeBuf.
//   - Third+ source: OR via runInline into mergeFallback, swapping
//     mergeBuf/mergeFallback when inline grows the container.
//
// The final AND with commonPos writes into mergeFallback (mode 0). After
// the per-key OR, mergeBuf and mergeFallback always point to different
// underlying buffers (any swaps stay paired), and `merged` is either a
// source slice or sits in mergeBuf — never in mergeFallback. So writing
// to mergeFallback never aliases merged. No third buffer needed.
//
// cursors must be zeroed by the caller.
func emitGroupFilteredN(
	bms []*Bitmap, groups [][]keyedMaskedEntry, cursors []int,
	commonPos, mergeBuf, mergeFallback []uint16, res *Bitmap,
) {
	n := len(bms)
	for {
		minKey, ok := nextEmitKey(groups, cursors)
		if !ok {
			return
		}

		// Build merged = OR of all containers at minKey across inputs.
		var merged []uint16
		mergedCount := 0
		for i := 0; i < n; i++ {
			if cursors[i] >= len(groups[i]) {
				continue
			}
			if groups[i][cursors[i]].originalKey != minKey {
				continue
			}
			c := bms[i].getContainer(groups[i][cursors[i]].offset)
			cursors[i]++

			switch mergedCount {
			case 0:
				merged = c
			case 1:
				merged = containerOrAlt(merged, c, mergeBuf, 0)
			default:
				if r := containerOrAlt(merged, c, mergeFallback, runInline); len(r) > 0 {
					mergeBuf, mergeFallback = mergeFallback, mergeBuf
					merged = r
				}
			}
			mergedCount++
		}

		filtered := containerAndAlt(merged, commonPos, mergeFallback, 0)
		if len(filtered) > 0 && getCardinality(filtered) > 0 {
			offset := res.newContainerNoClr(uint16(len(filtered)))
			copy(res.data[offset:], filtered)
			res.setKey(minKey, offset)
		}
	}
}

// nextEmitKey returns the smallest originalKey under any active cursor in
// groups, or (0, false) when every cursor has reached its group's end.
// Mirrors nextCommonGroup's role for the per-group emission walk: drives
// the N-way merge over per-input entry slices that share a common masked
// key. Does not mutate cursors — the caller advances them while collecting
// the containers at the returned key.
func nextEmitKey(groups [][]keyedMaskedEntry, cursors []int) (uint64, bool) {
	var minKey uint64
	found := false
	for i := 0; i < len(groups); i++ {
		if cursors[i] >= len(groups[i]) {
			continue
		}
		k := groups[i][cursors[i]].originalKey
		if !found || k < minKey {
			minKey = k
			found = true
		}
	}
	return minKey, found
}

func andMaskedContainersInRange(ra, b *Bitmap, entries []maskedEntry, ai, aj int) {
	// orBuf holds the accumulated OR result across the group.
	// fallbackBuf is needed only when inline OR fails (array+array that must
	// convert to bitmap): the result lands in fallbackBuf, which is then swapped
	// with orBuf so subsequent iterations continue to use orBuf as the target.
	orBuf := make([]uint16, maxContainerSize)
	fallbackBuf := make([]uint16, maxContainerSize)

	// Binary search to find the starting position in entries for this range.
	// The two-pointer optimisation used in the sequential path is not applicable
	// here since each goroutine starts at an arbitrary key.
	from, _ := slices.BinarySearchFunc(entries, ra.keys.key(ai), func(e maskedEntry, k uint64) int {
		if e.maskedKey < k {
			return -1
		}
		if e.maskedKey > k {
			return 1
		}
		return 0
	})

	for ; ai < aj; ai++ {
		ak := ra.keys.key(ai)
		ac := ra.getContainer(ra.keys.val(ai))

		var group []maskedEntry
		group, from = maskedEntriesFor(entries, ak, from)
		if group == nil {
			// No b key maps to ak under the mask — zero out ra's container.
			zeroOutContainer(ac)
			continue
		}

		// Build the OR of all b containers in the group.
		var orResult []uint16
		if len(group) == 1 {
			// Single container — use directly, no OR needed.
			orResult = b.getContainer(group[0].offset)
		} else {
			// Copy the first container into orBuf so we can OR into it in place.
			copy(orBuf, b.getContainer(group[0].offset))
			orResult = orBuf

			for i := 1; i < len(group); i++ {
				if c := containerOrAlt(orResult, b.getContainer(group[i].offset), fallbackBuf, runInline); len(c) > 0 {
					// Inline failed (array→bitmap conversion): result is in fallbackBuf.
					// Swap so orBuf always holds the current result.
					orBuf, fallbackBuf = fallbackBuf, orBuf
					orResult = c
				}
			}
		}

		// AND ra's container with the OR result, in place. AND keeps the
		// container size unchanged (only the number of elements decreases),
		// so inline always succeeds.
		if c := containerAndAlt(ac, orResult, nil, runInline); len(c) > 0 {
			panic("new container not expected in AndMasked inline mode")
		}
	}
}
