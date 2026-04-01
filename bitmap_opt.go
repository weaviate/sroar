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
