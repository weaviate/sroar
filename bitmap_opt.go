package sroar

import (
	"fmt"
	"math"
	"slices"
	"sync"
)

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

func (ra *Bitmap) And(bm *Bitmap) *Bitmap {
	if bm.IsEmpty() {
		ra.ZeroOut()
		return ra
	}

	andContainersInRange(ra, bm, 0, ra.keys.numKeys(), nil)
	return ra
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

		// AND ra's container with the OR result, in place. AND can only
		// shrink a container so inline always succeeds.
		if c := containerAndAlt(ac, orResult, nil, runInline); len(c) > 0 {
			panic("new container not expected in AndMasked inline mode")
		}
	}
}

// AndConc performs And merge concurrently.
// Concurrency is calculated based on number of internal containers
// in destination bitmap, so that each goroutine handles at least
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

	numContainers := ra.keys.numKeys()
	concurrency := calcConcurrency(numContainers, minContainersPerRoutine, maxConcurrency)
	callback := func(ai, aj, _ int) { andContainersInRange(ra, bm, ai, aj, nil) }
	concurrentlyInRanges(numContainers, concurrency, callback)
	return ra
}

func andContainersInRange(a, b *Bitmap, ai, aj int, optBuf []uint16) {
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
			ac := a.getContainer(off)
			zeroOutContainer(ac)
			ai++
		} else {
			bi++
		}
	}
	for ; ai < aj; ai++ {
		off := a.keys.val(ai)
		ac := a.getContainer(off)
		zeroOutContainer(ac)
	}
}

func AndNot(a, b *Bitmap) *Bitmap {
	res := NewBitmap()
	if a.IsEmpty() {
		return res
	}
	if b.IsEmpty() {
		return a.Clone()
	}

	buf := make([]uint16, maxContainerSize)
	andNotContainers(a, b, res, buf)
	return res
}

func andNotContainers(a, b, res *Bitmap, optBuf []uint16) {
	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	akToAc := map[uint64][]uint16{}
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
			if c := containerAndNotAlt(ac, bc, optBuf, 0); len(c) > 0 && getCardinality(c) > 0 {
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
			bi++
		}
	}
	for ; ai < an; ai++ {
		offset := a.keys.val(ai)
		ac := a.getContainer(offset)
		if getCardinality(ac) > 0 {
			ak := a.keys.key(ai)
			akToAc[ak] = ac
			sizeContainers += len(ac)
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
	}
}

func (ra *Bitmap) AndNot(bm *Bitmap) *Bitmap {
	if bm.IsEmpty() || ra.IsEmpty() {
		return ra
	}

	if numContainersA, numContainersB := ra.keys.numKeys(), bm.keys.numKeys(); numContainersB < numContainersA {
		andNotContainersInRangeB(ra, bm, 0, numContainersB, nil)
	} else {
		andNotContainersInRangeA(ra, bm, 0, numContainersA, nil)
	}

	return ra
}

// AndNotConc performs AndNot merge concurrently.
// Concurrency is calculated based on number of internal containers
// in source bitmap, so that each goroutine handles at least
// [minContainersPerRoutine] containers.
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

	numContainers := ra.keys.numKeys()
	andNotCallback := andNotContainersInRangeA
	if numContainersB := bm.keys.numKeys(); numContainersB < numContainers {
		numContainers = numContainersB
		andNotCallback = andNotContainersInRangeB
	}

	concurrency := calcConcurrency(numContainers, minContainersPerRoutine, maxConcurrency)
	callback := func(i, j, _ int) { andNotCallback(ra, bm, i, j, nil) }
	concurrentlyInRanges(numContainers, concurrency, callback)

	return ra
}

func andNotContainersInRangeA(a, b *Bitmap, ai, aj int, optBuf []uint16) {
	ak := a.keys.key(ai)
	bi := b.keys.search(ak)
	bn := b.keys.numKeys()
	andNotContainersInRange(a, b, ai, aj, bi, bn, optBuf)
}

func andNotContainersInRangeB(a, b *Bitmap, bi, bj int, optBuf []uint16) {
	bk := b.keys.key(bi)
	ai := a.keys.search(bk)
	an := a.keys.numKeys()
	andNotContainersInRange(a, b, ai, an, bi, bj, optBuf)
}

func andNotContainersInRange(a, b *Bitmap, ai, aj, bi, bj int, optBuf []uint16) {
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
			ai++
		} else {
			bi++
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

func (ra *Bitmap) Or(bm *Bitmap) *Bitmap {
	if bm.IsEmpty() {
		return ra
	}

	orContainersInRange(ra, bm, 0, bm.keys.numKeys())
	return ra
}

func orContainersInRange(a, b *Bitmap, bi, bn int) {
	buf := make([]uint16, maxContainerSize)

	bk := b.keys.key(bi)
	ai := a.keys.search(bk)
	an := a.keys.numKeys()

	// copy containers from b to a all at once
	// expanding underlying data slice and keys subslice once
	sizeContainers := 0
	newKeys := 0
	bKeys := []uint64{}
	bContainers := [][]uint16{}

	for ai < an && bi < bn {
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
			ai++
		} else {
			off := b.keys.val(bi)
			bc := b.getContainer(off)
			if getCardinality(bc) > 0 {
				bKeys = append(bKeys, bk)
				bContainers = append(bContainers, bc)
				sizeContainers += len(bc)
				newKeys++
			}
			bi++
		}
	}

	// extend bKeys and bContainers to fit all remaining data
	// (once instead of multiple times by calling append)
	if diff := bn - bi; diff > 0 {
		if cp, ln := cap(bKeys), len(bKeys); cp-ln < diff {
			bKeysCopy := make([]uint64, ln, ln+diff)
			copy(bKeysCopy, bKeys)
			bKeys = bKeysCopy
		}
		if cp, ln := cap(bContainers), len(bContainers); cp-ln < diff {
			bContainersCopy := make([][]uint16, ln, ln+diff)
			copy(bContainersCopy, bContainers)
			bContainers = bContainersCopy
		}

		for ; bi < bn; bi++ {
			off := b.keys.val(bi)
			bc := b.getContainer(off)
			if getCardinality(bc) > 0 {
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

// OrConc performs Or merge concurrently.
// Concurrency is calculated based on number of internal containers
// in source bitmap, so that each goroutine handles at least
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

	numContainers := bm.keys.numKeys()
	concurrency := calcConcurrency(numContainers, minContainersPerRoutine, maxConcurrency)

	if concurrency <= 1 {
		orContainersInRange(ra, bm, 0, numContainers)
		return ra
	}

	var totalNewKeys int
	var totalSizeContainers int
	allKeys := make([][]uint64, concurrency)
	allContainers := make([][][]uint16, concurrency)
	lock := new(sync.Mutex)
	callback := func(bi, bj, i int) {
		newKeys, sizeContainers, keys, containers := orContainersInRangeConc(ra, bm, bi, bj)

		lock.Lock()
		totalNewKeys += newKeys
		totalSizeContainers += sizeContainers
		lock.Unlock()
		allKeys[i] = keys
		allContainers[i] = containers
	}
	concurrentlyInRanges(numContainers, concurrency, callback)
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

func orContainersInRangeConc(a, b *Bitmap, bi, bn int,
) (newKeys, sizeContainers int, bKeys []uint64, bContainers [][]uint16) {
	buf := make([]uint16, maxContainerSize)

	bk := b.keys.key(bi)
	ai := a.keys.search(bk)
	an := a.keys.numKeys()

	// copy containers from b to a all at once
	// expanding underlying data slice and keys subslice once
	sizeContainers = 0
	newKeys = 0
	bKeys = []uint64{}
	bContainers = [][]uint16{}

	for ai < an && bi < bn {
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
			ai++
		} else {
			off := b.keys.val(bi)
			bc := b.getContainer(off)
			if getCardinality(bc) > 0 {
				bKeys = append(bKeys, bk)
				bContainers = append(bContainers, bc)
				sizeContainers += len(bc)
				newKeys++
			}
			bi++
		}
	}

	// extend bKeys and bContainers to fit all remaining data
	// (once instead of multiple times by calling append)
	if diff := bn - bi; diff > 0 {
		if cp, ln := cap(bKeys), len(bKeys); cp-ln < diff {
			bKeysCopy := make([]uint64, ln, ln+diff)
			copy(bKeysCopy, bKeys)
			bKeys = bKeysCopy
		}
		if cp, ln := cap(bContainers), len(bContainers); cp-ln < diff {
			bContainersCopy := make([][]uint16, ln, ln+diff)
			copy(bContainersCopy, bContainers)
			bContainers = bContainersCopy
		}

		for ; bi < bn; bi++ {
			off := b.keys.val(bi)
			bc := b.getContainer(off)
			if getCardinality(bc) > 0 {
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

func calcConcurrency(numContainers, minContainers, maxConcurrency int) int {
	concurrency := numContainers / minContainers
	if concurrency < 1 || maxConcurrency == 1 {
		concurrency = 1
	} else if maxConcurrency > 1 && maxConcurrency < concurrency {
		concurrency = maxConcurrency
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
			go func() {
				callback(from, to, i)
				wg.Done()
			}()
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

func (dst *Bitmap) CompareNumKeys(src *Bitmap) int {
	if dst == nil && src == nil {
		return 0
	}
	if src == nil {
		return 1
	}
	if dst == nil {
		return -1
	}
	if dstN, srcN := dst.keys.numKeys(), src.keys.numKeys(); dstN > srcN {
		return 1
	} else if dstN < srcN {
		return -1
	} else {
		return 0
	}
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
	c := cap(buf)
	dstbuf := buf[:c]
	if c%2 != 0 {
		dstbuf = buf[:c-1]
	}

	src := ra
	if ra == nil {
		src = NewBitmap()
	}

	srclen := src.LenInBytes()
	if srclen > len(dstbuf) {
		panic(fmt.Sprintf("Buffer too small, given %d, required %d", cap(buf), srclen))
	}

	srcbuf := toByteSlice(src.data)
	copy(dstbuf, srcbuf)

	// adjust length to src length, keep capacity as entire buffer
	bm := FromBuffer(dstbuf)
	bm.data = bm.data[:srclen/2]
	return bm
}

// FromBufferUnlimited returns a pointer to bitmap corresponding to the given buffer.
// Entire buffer capacity is utlized for future bitmap modifications and expansions.
func FromBufferUnlimited(buf []byte) *Bitmap {
	ln := len(buf)
	assert(ln%2 == 0)
	if len(buf) < 8 {
		return NewBitmap()
	}

	cp := cap(buf)
	data := buf[:cp]
	if cp%2 != 0 {
		data = buf[:cp-1]
	}

	du := byteTo16SliceUnsafe(data)
	x := uint16To64SliceUnsafe(du[:4])[indexNodeSize]
	return &Bitmap{
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
			bm = newBitampToBuf(minimalKeysLen, maxContainerSize, ra.data)
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

	sizeKeys := 8 * newKeys // 2x uint64 (key+offset) = 8x uint16
	if ra.keys.numKeys()+newKeys < ra.keys.maxKeys() {
		if ln+sizeContainers <= cp {
			// keys and containers fit. nothing to do
			return
		}
		// keys fit, containers do not. expand slice to make room for containers and only new keys
	} else {
		if ln+sizeKeys+sizeContainers <= cp {
			curNumKeys := ra.keys.numKeys()

			// keys do not fit, containers do. just move containers to make room for keys
			if curNumKeys > newKeys {
				// make room for up to curNumKeys additional keys
				sizeKeys = 8 * curNumKeys // 2x uint64 (key+offset) = 8x uint16
				if left := cp - ln - sizeContainers; left < sizeKeys {
					sizeKeys = left / 8 * 8
				}
			}

			// new containers will fit. just move containers to make room for keys
			curSizeKeys := ra.keys.size()
			newSizeKeys := curSizeKeys + sizeKeys

			ra.data = ra.data[:ln+sizeKeys]
			n := copy(ra.data[newSizeKeys:], ra.data[curSizeKeys:])
			ra.memMoved += n
			clear(ra.data[curSizeKeys:newSizeKeys]) // Zero out the space in the middle.

			ra.keys = uint16To64SliceUnsafe(ra.data[:newSizeKeys])
			ra.keys.setNodeSize(newSizeKeys)
			for i := 0; i < curNumKeys; i++ {
				ra.keys.setAt(valOffset(i), ra.keys.val(i)+uint64(sizeKeys))
			}
			return
		}
		// neither keys nor containers fit. expand slice to make room for containers and more keys
		sizeKeys = 8 * max(newKeys, ra.keys.numKeys()) // 2x uint64 (key+offset) = 8x uint16
	}

	// expand 2x (or up to sizeKeys+sizeNewContainers if 2x is too little)
	growBy := max(cp, sizeKeys+sizeContainers)
	out := make([]uint16, ln+sizeKeys, cp+growBy)

	curSizeKeys := ra.keys.size()
	newSizeKeys := curSizeKeys + sizeKeys
	copy(out, ra.data[:curSizeKeys])
	copy(out[newSizeKeys:], ra.data[curSizeKeys:])
	ra.data = out
	ra._ptr = nil // Allow Go to GC whatever this was pointing to.
	// Re-reference ra.keys correctly because underlying array has changed.

	ra.keys = uint16To64SliceUnsafe(ra.data[:newSizeKeys])
	ra.keys.setNodeSize(newSizeKeys)
	for i := 0; i < ra.keys.numKeys(); i++ {
		ra.keys.setAt(valOffset(i), ra.keys.val(i)+uint64(sizeKeys))
	}
}

// Masked applies the given mask to every key and returns a new bitmap.
// Keys that collapse to the same masked value have their containers merged
// via container-level OR operations. The lowest 16 bits of keys (used for
// dimensions in Dim-aware bitmaps) are always zeroed, collapsing all
// dimensions implicitly.
func (ra *Bitmap) Masked(mask uint64) *Bitmap {
	return ra.maskedInto(mask, NewBitmap())
}

// MaskedToBuf is like Masked but uses the provided byte slice as the
// underlying buffer for the result bitmap, avoiding heap allocation when
// the buffer is large enough. The lowest 16 bits of keys are always zeroed.
func (ra *Bitmap) MaskedToBuf(mask uint64, buf []byte) *Bitmap {
	return ra.maskedInto(mask, NewBitmapToBuf(buf))
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
	minKeys := an
	if bn < minKeys {
		minKeys = bn
	}
	res.expandConditionally(minKeys, 0)

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

				roff, has := res.keys.getValue(maskedKey)
				if !has {
					// First container for this masked key — copy it directly.
					res.expandConditionally(0, len(c))
					roff = res.newContainerNoClr(uint16(len(c)))
					copy(res.data[roff:], c)
					res.setKey(maskedKey, roff)
				} else {
					// Merge with the existing container via OR.
					rc := res.getContainer(roff)
					if out := containerOrAlt(rc, c, orBuf, runInline); len(out) > 0 {
						// Inline failed (container grew). If the old container
						// is at the end of res.data, trim and regrow in place to
						// avoid dead space. Otherwise append (dead space).
						if roff+uint64(len(rc)) == uint64(len(res.data)) {
							res.data = res.data[:roff]
						}
						res.expandConditionally(0, len(out))
						roff = res.newContainerNoClr(uint16(len(out)))
						copy(res.data[roff:], out)
						res.setKey(maskedKey, roff)
					}
				}
			}
			ai++
			bi++
		} else if ak < bk {
			ai++
		} else {
			bi++
		}
	}
	return res
}

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

		boff, has := b.keys.getValue(maskedKey)
		if !has {
			// First container for this masked key — copy it directly.
			b.expandConditionally(0, len(ac))
			boff = b.newContainerNoClr(uint16(len(ac)))
			copy(b.data[boff:], ac)
			b.setKey(maskedKey, boff)
		} else {
			// Merge with the existing container via OR.
			bc := b.getContainer(boff)
			if c := containerOrAlt(bc, ac, buf, runInline); len(c) > 0 {
				// Inline failed (container grew). If the old container
				// is at the end of b.data, trim and regrow in place to
				// avoid dead space. Otherwise append (dead space).
				if boff+uint64(len(bc)) == uint64(len(b.data)) {
					b.data = b.data[:boff]
				}
				b.expandConditionally(0, len(c))
				boff = b.newContainerNoClr(uint16(len(c)))
				copy(b.data[boff:], c)
				b.setKey(maskedKey, boff)
			}
		}
	}
	return b
}
