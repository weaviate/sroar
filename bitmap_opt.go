package sroar

import (
	"fmt"
	"math"
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

func AndBuf(a, b *Bitmap, buf []uint16) *Bitmap {
	assert(len(buf) == maxContainerSize)

	res := NewBitmap()
	if a.IsEmpty() || b.IsEmpty() {
		return res
	}

	andContainers(a, b, res, buf)
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
		ra.Reset()
		return ra
	}

	andContainersInRange(ra, bm, 0, ra.keys.numKeys(), nil)
	return ra
}

func (ra *Bitmap) AndBuf(bm *Bitmap, buf []uint16) *Bitmap {
	assert(len(buf) == maxContainerSize)

	if bm.IsEmpty() {
		ra.Reset()
		return ra
	}

	andContainersInRange(ra, bm, 0, ra.keys.numKeys(), buf)
	return ra
}

func andContainersInRange(a, b *Bitmap, ai, an int, optBuf []uint16) {
	ak := a.keys.key(ai)
	bi := b.keys.search(ak)
	bn := b.keys.numKeys()

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			aoff := a.keys.val(ai)
			ac := a.getContainer(aoff)
			boff := b.keys.val(bi)
			bc := b.getContainer(boff)
			if c := containerAndAlt(ac, bc, optBuf, runInline); len(c) > 0 {
				// make room for container, replacing smaller one and update key offset to new container.
				a.insertAt(aoff, c)
				a.setKey(ak, aoff)
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
	for ; ai < an; ai++ {
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

	andNotContainers(a, b, res, nil)
	return res
}

func AndNotBuf(a, b *Bitmap, buf []uint16) *Bitmap {
	assert(len(buf) == maxContainerSize)

	res := NewBitmap()
	if a.IsEmpty() {
		return res
	}
	if b.IsEmpty() {
		return a.Clone()
	}

	andNotContainers(a, b, res, buf)
	return res
}

func andNotContainers(a, b, res *Bitmap, optBuf []uint16) {
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
				// create a new container and update the key offset to this container.
				offset := res.newContainerNoClr(uint16(len(ac)))
				copy(res.data[offset:], ac)
				res.setKey(ak, offset)
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
			// create a new container and update the key offset to this container.
			offset = res.newContainerNoClr(uint16(len(ac)))
			copy(res.data[offset:], ac)
			res.setKey(ak, offset)
		}
	}
}

func (ra *Bitmap) AndNot(bm *Bitmap) *Bitmap {
	if bm.IsEmpty() || ra.IsEmpty() {
		return ra
	}

	andNotContainersInRange(ra, bm, 0, bm.keys.numKeys(), nil)
	return ra
}

func (ra *Bitmap) AndNotBuf(bm *Bitmap, buf []uint16) *Bitmap {
	assert(len(buf) == maxContainerSize)

	if bm.IsEmpty() || ra.IsEmpty() {
		return ra
	}

	andNotContainersInRange(ra, bm, 0, bm.keys.numKeys(), buf)
	return ra
}

func andNotContainersInRange(a, b *Bitmap, bi, bn int, optBuf []uint16) {
	bk := b.keys.key(bi)
	ai := a.keys.search(bk)
	an := a.keys.numKeys()

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			aoff := a.keys.val(ai)
			ac := a.getContainer(aoff)
			boff := b.keys.val(bi)
			bc := b.getContainer(boff)
			if c := containerAndNotAlt(ac, bc, optBuf, runInline); len(c) > 0 {
				// make room for container, replacing smaller one and update key offset to new container.
				a.insertAt(aoff, c)
				a.setKey(ak, aoff)
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

func OrBuf(a, b *Bitmap, buf []uint16) *Bitmap {
	assert(len(buf) == maxContainerSize)

	res := NewBitmap()
	if ae, be := a.IsEmpty(), b.IsEmpty(); ae && be {
		return res
	} else if ae {
		return b.Clone()
	} else if be {
		return a.Clone()
	}

	orContainers(a, b, res, buf)
	return res
}

func orContainers(a, b, res *Bitmap, buf []uint16) {
	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	akToAc := map[uint64][]uint16{}
	bkToBc := map[uint64][]uint16{}
	sizeContainers := uint64(0)
	sizeKeys := uint64(0)

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
				sizeContainers += uint64(len(ac))
				sizeKeys += 8 // 2x uint64 = 8x uint16; for key and offset
			}
			ai++
		} else {
			off := b.keys.val(bi)
			bc := b.getContainer(off)
			if getCardinality(bc) > 0 {
				bkToBc[bk] = bc
				sizeContainers += uint64(len(bc))
				sizeKeys += 8 // 2x uint64 = 8x uint16; for key and offset
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
			sizeContainers += uint64(len(ac))
			sizeKeys += 8 // 2x uint64 = 8x uint16; for key and offset
		}
	}
	for ; bi < bn; bi++ {
		off := b.keys.val(bi)
		bc := b.getContainer(off)
		if getCardinality(bc) > 0 {
			bk := b.keys.key(bi)
			bkToBc[bk] = bc
			sizeContainers += uint64(len(bc))
			sizeKeys += 8 // 2x uint64 = 8x uint16; for key and offset
		}
	}

	if sizeContainers > 0 {
		// ensure enough space for new containers and keys,
		// allocate required memory just once avoid copying underlying data slice multiple times
		res.expandNoLengthChange(sizeContainers + sizeKeys)
		res.expandKeys(sizeKeys)

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

	buf := make([]uint16, maxContainerSize)
	orContainersInRange(ra, bm, 0, bm.keys.numKeys(), buf)
	return ra
}

func (ra *Bitmap) OrBuf(bm *Bitmap, buf []uint16) *Bitmap {
	assert(len(buf) == maxContainerSize)

	if bm.IsEmpty() {
		return ra
	}

	orContainersInRange(ra, bm, 0, bm.keys.numKeys(), buf)
	return ra
}

func orContainersInRange(a, b *Bitmap, bi, bn int, buf []uint16) {
	bk := b.keys.key(bi)
	ai := a.keys.search(bk)
	an := a.keys.numKeys()

	// copy containers from b to a all at once
	// expanding underlying data slice and keys subslice once
	bkToBc := map[uint64][]uint16{}
	sizeContainers := uint64(0)
	sizeKeys := uint64(0)

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
				bkToBc[bk] = bc
				sizeContainers += uint64(len(bc))
				sizeKeys += 8 // 2x uint64 = 8x uint16; for key and offset
			}
			bi++
		}
	}
	for ; bi < bn; bi++ {
		off := b.keys.val(bi)
		bc := b.getContainer(off)
		if getCardinality(bc) > 0 {
			bk := b.keys.key(bi)
			bkToBc[bk] = bc
			sizeContainers += uint64(len(bc))
			sizeKeys += 8 // 2x uint64 = 8x uint16; for key and offset
		}
	}

	if sizeContainers > 0 {
		// ensure enough space for new containers and keys,
		// allocate required memory just once to avoid copying underlying data slice multiple times
		a.expandNoLengthChange(sizeContainers + sizeKeys)
		a.expandKeys(sizeKeys)

		for bk, bc := range bkToBc {
			// create a new container and update the key offset to this container.
			offset := a.newContainerNoClr(uint16(len(bc)))
			copy(a.data[offset:], bc)
			a.setKey(bk, offset)
		}
	}
}

const minContainersForConcurrency = 16

// AndToSuperset calculates intersection of current and incoming bitmap
// It reuses containers present in current bitmap
// and utilize container buffers provided.
// Number of passed buffers indicates concurrency level
// (e.g. 4 buffers = merge will be performed by 4 goroutines).
//
// CAUTION: should be used only when current bitmap contained before
// all elements present in incoming bitmap
func (dst *Bitmap) AndToSuperset(src *Bitmap, containerBufs ...[]uint16) {
	conc := len(containerBufs)
	assert(conc > 0)

	dstNumKeys := dst.keys.numKeys()
	if src == nil {
		concurrentlyOnRange(conc, dstNumKeys, func(_, from, to int) {
			zeroOutSelectedContainers(dst, from, to)
		})
		return
	}

	srcNumKeys := src.keys.numKeys()
	concurrentlyOnRange(conc, dstNumKeys, func(i, from, to int) {
		andSelectedContainers(dst, src, from, to, 0, srcNumKeys, containerBufs[i])
	})
}

// OrToSuperset calculates union of current and incoming bitmap
// It reuses containers present in current bitmap
// and utilize containers buffers provided.
// Number of passed buffers indicates concurrency level
// (e.g. 4 buffers = merge will be performed by 4 goroutines).
//
// CAUTION: should be used only when current bitmap contained before
// all elements present in incoming bitmap
func (dst *Bitmap) OrToSuperset(src *Bitmap, containerBufs ...[]uint16) {
	conc := len(containerBufs)
	assert(conc > 0)

	if src == nil {
		return
	}

	srcNumKeys := src.keys.numKeys()
	concurrentlyOnRange(conc, srcNumKeys, func(i, from, to int) {
		orSelectedContainers(dst, src, from, to, containerBufs[i])
	})
}

// AndNotToSuperset calculates difference between current and incoming bitmap
// It reuses containers present in current bitmap
// and utilize containers buffers provided.
// Number of passed buffers indicates concurrency level
// (e.g. 4 buffers = merge will be performed by 4 goroutines).
//
// CAUTION: should be used only when current bitmap contained before
// all elements present in incoming bitmap
func (dst *Bitmap) AndNotToSuperset(src *Bitmap, containerBufs ...[]uint16) {
	conc := len(containerBufs)
	assert(conc > 0)

	if src == nil {
		return
	}

	dstNumKeys := dst.keys.numKeys()
	srcNumKeys := src.keys.numKeys()
	concurrentlyOnRange(conc, dstNumKeys, func(i, from, to int) {
		andNotSelectedContainers(dst, src, from, to, 0, srcNumKeys, containerBufs[i])
	})
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

func concurrentlyOnRange(conc, max int, callback func(i, from, to int)) {
	if conc == 1 || max < conc*minContainersForConcurrency {
		callback(0, 0, max)
		return
	}

	delta := max / conc

	wg := new(sync.WaitGroup)
	wg.Add(conc - 1)
	for i := 0; i < conc-1; i++ {
		go func(i int) {
			callback(i, delta*i, delta*(i+1))
			wg.Done()
		}(i)
	}
	callback(conc-1, delta*(conc-1), max)
	wg.Wait()
}

func zeroOutSelectedContainers(a *Bitmap, ai, an int) {
	for ; ai < an; ai++ {
		off := a.keys.val(ai)
		zeroOutContainer(a.getContainer(off))
	}
}

func andSelectedContainers(a, b *Bitmap, ai, an, bi, bn int, containerBuf []uint16) {
	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)

			if getCardinality(bc) == 0 {
				zeroOutContainer(ac)
			} else {
				containerAndToSuperset(ac, bc, containerBuf)
			}
			ai++
			bi++
		} else if ak < bk {
			off := a.keys.val(ai)
			zeroOutContainer(a.getContainer(off))
			ai++
		} else {
			bi++
		}
	}
	for ; ai < an; ai++ {
		off := a.keys.val(ai)
		zeroOutContainer(a.getContainer(off))
	}
}

func orSelectedContainers(a, b *Bitmap, bi, bn int, containerBuf []uint16) {
	for ; bi < bn; bi++ {
		off := b.keys.val(bi)
		bc := b.getContainer(off)
		if getCardinality(bc) == 0 {
			continue
		}

		bk := b.keys.key(bi)
		ai := a.keys.search(bk)
		if ai >= a.keys.numKeys() || a.keys.key(ai) != bk {
			// Container does not exist in dst.
			panic("Current bitmap should have all containers of incoming bitmap")
		} else {
			// Container exists in dst as well. Do an inline containerOr.
			off = a.keys.val(ai)
			ac := a.getContainer(off)
			containerOrToSuperset(ac, bc, containerBuf)
		}
	}
}

func andNotSelectedContainers(a, b *Bitmap, ai, an, bi, bn int, containerBuf []uint16) {
	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := b.keys.val(bi)
			bc := b.getContainer(off)
			if getCardinality(bc) != 0 {
				off = a.keys.val(ai)
				ac := a.getContainer(off)
				containerAndNotToSuperset(ac, bc, containerBuf)
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

// Prefill creates bitmap prefilled with elements [0-maxX]
func Prefill(maxX uint64) *Bitmap {
	n, rem := prefillNoOfFullContAndRem(maxX)

	// create additional container for remaining values
	// (or reserve space for new one if there are not any remaining)
	// +1 additional key to avoid keys expanding (there should always be 1 spare)
	bm := newBitmapWithSize(int(n)+1+1, maxContainerSize, int(n)*maxContainerSize)
	prefill(n, rem, bm)
	return bm
}

func prefill(noFullContainers, remainder uint64, bm *Bitmap) {
	var refContainer []uint16

	if noFullContainers > 0 {
		refContainer = bm.getContainer(bm.keys.val(0))
		refContainer[indexSize] = maxContainerSize
		refContainer[indexType] = typeBitmap
		setCardinality(refContainer, maxCardinality)

		// fill entire bitmap container with ones
		refContainer64 := uint16To64SliceUnsafe(refContainer[startIdx:])
		for i := range refContainer64 {
			refContainer64[i] = math.MaxUint64
		}

		// fill remaining containers by copying reference one
		for i := uint64(1); i < noFullContainers; i++ {
			key := (i * uint64(maxCardinality)) & mask
			offset := bm.newContainerNoClr(maxContainerSize)
			bm.setKey(key, offset)

			copy(bm.data[offset:], refContainer)
		}
	}

	if remainder > 0 {
		var remOffset uint64
		if noFullContainers > 0 {
			// create container for remaining values
			key := (noFullContainers * uint64(maxCardinality)) & mask
			remOffset = bm.newContainer(maxContainerSize)
			bm.setKey(key, remOffset)
		} else {
			// get first container
			remOffset = bm.keys.val(0)
		}

		// fmt.Printf("  ==> remainder [%d][%d]\n", remainder, int(remainder))

		container := bm.getContainer(remOffset)
		container[indexSize] = maxContainerSize
		container[indexType] = typeBitmap
		setCardinality(container, int(remainder))
		bitmap(container).setRange(0, int(remainder)-1, refContainer)
	}
}

func prefillNoOfFullContAndRem(maxX uint64) (uint64, uint64) {
	maxCard64 := uint64(maxCardinality)

	// maxX should be included, therefore +1
	n := maxX / maxCard64
	rem := maxX % maxCard64
	if rem == maxCard64-1 {
		n++
	}
	rem = (rem + 1) % maxCard64
	// fmt.Printf("  ==> maxX [%d] n [%d] rem [%d]\n", maxX, n, rem)
	return n, rem
}

func (b bitmap) setRange(leftY, rightY int, onesBitmap bitmap) {
	leftY16 := (leftY + 15) / 16 * 16
	rightY16 := (rightY + 1) / 16 * 16

	// fmt.Printf("  ==> maxYCur [%d] l16 [%d] l64 [%d]\n", leftY, leftY16, leftY64)
	// fmt.Printf("  ==> maxY [%d] r16 [%d] r64 [%d]\n", rightY, rightY16, rightY64)

	container16 := b[startIdx:]
	if onesBitmap != nil {
		l := uint16(leftY16 / 16)
		r := uint16(rightY16 / 16)
		copy(container16[l:r], onesBitmap[startIdx+l:startIdx+r])
	} else {
		leftY64 := (leftY + 63) / 64 * 64
		rightY64 := (rightY + 1) / 64 * 64

		if l, r := leftY64/64, rightY64/64; l < r {
			container64 := uint16To64SliceUnsafe(container16)
			for i := l; i < r; i++ {
				container64[i] = math.MaxUint64
			}
		}
		for i, r := leftY16/16, leftY64/16; i < r; i++ {
			container16[i] = math.MaxUint16
		}
		for i, r := rightY64/16, rightY16/16; i < r; i++ {
			container16[i] = math.MaxUint16
		}
	}
	for y, r := leftY, leftY16; y < r; y++ {
		container16[y/16] |= bitmapMask[y%16]
	}
	for y, r := rightY16, rightY; y <= r; y++ {
		container16[y/16] |= bitmapMask[y%16]
	}
}

func (ra *Bitmap) FillUp(maxX uint64) {
	if ra == nil {
		return
	}

	n, rem := prefillNoOfFullContAndRem(maxX)
	if ra.IsEmpty() {
		// if rem == 0 try to fit data into existing memory
		// if there is not enough space anyway, allocate enough memory to fit additional container
		minNoContainers := int(n)
		if rem > 0 {
			minNoContainers++
		}
		minKeysLen := calculateInitialKeysLen(minNoContainers + 1)
		minContainersLen := minNoContainers * maxContainerSize
		minLen := minKeysLen + minContainersLen

		var bm *Bitmap
		if minLen <= cap(ra.data) {
			bm = newBitampWithBuf(minKeysLen, maxContainerSize, ra.data)
		} else {
			optNoContainers := int(n + 1)
			optKeysLen := minKeysLen
			optLen := minLen
			if optNoContainers != minNoContainers {
				optKeysLen = calculateInitialKeysLen(optNoContainers + 1)
				optContainersLen := optNoContainers * maxContainerSize
				optLen = optKeysLen + optContainersLen
			}
			buf := make([]uint16, optLen)
			bm = newBitampWithBuf(optKeysLen, maxContainerSize, buf)
		}
		prefill(n, rem, bm)
		ra.data = bm.data
		ra._ptr = bm._ptr
		ra.keys = bm.keys
		return
	}

	maxXCur := ra.Maximum()
	if maxXCur >= maxX {
		return
	}

	nCur, remCur := prefillNoOfFullContAndRem(maxXCur)
	maxXKey := maxX & mask
	maxXCurKey := maxXCur & mask

	// same container
	if maxXKey == maxXCurKey {
		maxY := int(uint16(maxX))
		maxYCur := int(uint16(maxXCur))

		i := ra.keys.searchRev(maxXKey)
		offset := ra.keys.val(i)
		commonContainer := ra.getContainer(offset)

		switch commonContainer[indexType] {
		case typeBitmap:
			bitmap(commonContainer).setRange(maxYCur, maxY, nil)
		case typeArray:

		default:
			panic("unknown container type")
		}

		newCard := getCardinality(commonContainer) + maxY - maxYCur
		setCardinality(commonContainer, newCard)
		return
	}

	/*

		 	if maxX and curMaxX in same container
				if container bitmap
					merge values
				if container array
					if fits all values
						merge values
					if does not fit all values
						expand bitmap
						convert to bitmap
						merge values

			if maxX and curMaxX in different containers
				calculate required size






				if curMaxX container full




	*/

	curNoContainers := nCur
	noContainers := n
	if rem > 0 {
		noContainers++
	}
	requiredContainers := noContainers - curNoContainers

	var commonContainer []uint16
	mergeCommonBitmap := false
	mergeCommonArray := false
	convertCommonArrayToBitmap := false

	startN := nCur
	if remCur > 0 {
		startN++

		commonKey := maxXCur & mask
		var offset uint64
		for i := ra.keys.numKeys() - 1; i >= 0; i-- {
			if commonKey == ra.keys.key(i) {
				offset = ra.keys.val(i)
			}
		}
		commonContainer = ra.getContainer(offset)
		switch commonContainer[indexType] {
		case typeBitmap:
			requiredContainers--
			mergeCommonBitmap = true
		case typeArray:
			left := int(commonContainer[indexSize]) - getCardinality(commonContainer)
			needed := maxCardinality - int(remCur)
			if needed <= left {
				mergeCommonArray = true
				requiredContainers--
			} else {
				convertCommonArrayToBitmap = true
			}
		}
	}

	requiredContainersLen := requiredContainers * maxContainerSize
	requiredKeysLen := requiredContainers * 2 * 4
	requiredLen := requiredContainersLen + requiredKeysLen

	ra.expandNoLengthChange(requiredLen)

	var refContainer []uint16
	if startN < n {
		key := (startN * uint64(maxCardinality)) & mask
		offset := ra.newContainerNoClr(maxContainerSize)
		ra.setKey(key, offset)

		refContainer = ra.getContainer(offset)[:maxContainerSize]
		refContainer[indexSize] = maxContainerSize
		refContainer[indexType] = typeBitmap
		setCardinality(refContainer, maxCardinality)

		// fill entire bitmap container with ones
		refContainer64 := uint16To64SliceUnsafe(refContainer[startIdx:])
		for i := range refContainer64 {
			refContainer64[i] = math.MaxUint64
		}

		// fill remaining containers by copying reference one
		for i := startN + 1; i < n; i++ {
			key = (i * uint64(maxCardinality)) & mask
			offset = ra.newContainerNoClr(maxContainerSize)
			ra.setKey(key, offset)

			copy(ra.data[offset:], refContainer)
		}
	}

	if rem > 0 {
		// create container for remaining values
		key := (n * uint64(maxCardinality)) & mask
		offset := ra.newContainer(maxContainerSize)
		ra.setKey(key, offset)

		container := ra.getContainer(offset)
		container[indexSize] = maxContainerSize
		container[indexType] = typeBitmap
		setCardinality(container, int(rem))

		n16 := uint16(rem) / 16
		rem16 := uint16(rem) % 16

		if refContainer != nil {
			// refContainer available (maxX >= math.MaxUint16-1),
			// fill remaining values container by copying biggest possible slice of refContainer (batches of 16s)
			copy(ra.data[offset+uint64(startIdx):], refContainer[startIdx:startIdx+n16])
			// set remaining bits
			for i := uint16(0); i < rem16; i++ {
				container[startIdx+n16] |= bitmapMask[i]
			}
		} else {
			// refContainer not available (maxX < math.MaxUint16-1),
			// set bits by copying MaxUint64 first, then MaxUint16, then single bits
			n64 := uint16(rem) / 64

			container64 := uint16To64SliceUnsafe(container[startIdx:])
			for i := uint16(0); i < n64; i++ {
				container64[i] = math.MaxUint64
			}
			for i := uint16(n64 * 4); i < n16; i++ {
				container[startIdx+i] = math.MaxUint16
			}
			for i := uint16(0); i < rem16; i++ {
				container[startIdx+n16] |= bitmapMask[i]
			}
		}
	}

	if mergeCommonBitmap {

	}
	if mergeCommonArray {

	}
	if convertCommonArrayToBitmap {

	}
}

/*
	fillup:

	- if empty
		- if size < required - replace with new prefilled
		- if size >= required - reuse memory
			reset memory, create prefilled from scratch
	- if filled






		- if size < required - replace with new prefilled
		- if size >= required - reuse memory
			- if will fit new containers and last one as bitmap
				convert last to bitmap (if needed)
				add missing containers as bitmaps
			- if not
				reset memory, create prefilled from scratch

*/

func (ra *Bitmap) LenBytes() int {
	if ra == nil {
		return 0
	}
	return len(ra.data) * 2
}

func (ra *Bitmap) CapBytes() int {
	if ra == nil {
		return 0
	}
	return cap(ra.data) * 2
}

func (ra *Bitmap) CloneToBuf(buf []byte) *Bitmap {
	if len(buf)%2 != 0 {
		panic(fmt.Sprintf("Buffer size should be even, given %d", len(buf)))
	}

	a := ra
	if ra == nil {
		a = NewBitmap()
	}

	if alen := a.LenBytes(); alen > len(buf) {
		panic(fmt.Sprintf("Buffer too small, given %d, required %d", len(buf), alen))
	}

	abuf := toByteSlice(a.data)
	copy(buf, abuf)
	return FromBuffer(buf)
}
