package sroar

import (
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
