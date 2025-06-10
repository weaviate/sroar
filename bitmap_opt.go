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
		ra.Reset()
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
				sizeContainers += uint64(len(ac))
				sizeKeys += 8 // 2x uint64 = 8x uint16; for key and offset
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
			sizeContainers += uint64(len(ac))
			sizeKeys += 8 // 2x uint64 = 8x uint16; for key and offset
		}
	}

	if sizeContainers > 0 {
		// ensure enough space for new containers and keys,
		// allocate required memory just once to avoid copying underlying data slice multiple times
		res.expandNoLengthChange(sizeContainers + sizeKeys)
		res.expandKeys(sizeKeys)

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

	andNotContainersInRange(ra, bm, 0, bm.keys.numKeys(), nil)
	return ra
}

// AndNotConc performs AndNot merge concurrently.
// Concurrency is calculated based on number of internal containers
// in source bitmap, so that each goroutine handles at least
// [minContainersPerRoutine] containers.
// maxConcurrency limits concurrency calculated internally.
// If maxConcurrency <= 0, then calculated concurrency is not limited.
//
// E.g.: src bitmap has 100 containers. Internal concurrency = 100/24 = 4. For:
// - maxConcurrency = 2, there will be 2 goroutines executed
// - maxConcurrency = 6, there will be 4 goroutines executed
func (ra *Bitmap) AndNotConc(bm *Bitmap, maxConcurrency int) *Bitmap {
	if bm.IsEmpty() || ra.IsEmpty() {
		return ra
	}

	numContainers := bm.keys.numKeys()
	concurrency := calcConcurrency(numContainers, minContainersPerRoutine, maxConcurrency)
	callback := func(bi, bj, _ int) { andNotContainersInRange(ra, bm, bi, bj, nil) }
	concurrentlyInRanges(numContainers, concurrency, callback)
	return ra
}

func andNotContainersInRange(a, b *Bitmap, bi, bj int, optBuf []uint16) {
	bk := b.keys.key(bi)
	ai := a.keys.search(bk)
	an := a.keys.numKeys()

	for ai < an && bi < bj {
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
		buf := make([]uint16, maxContainerSize)
		orContainersInRange(ra, bm, 0, numContainers, buf)
		return ra
	}

	var totalSizeContainers, totalSizeKeys uint64
	var allKeys []uint64
	var allContainers [][]uint16
	lock := new(sync.Mutex)
	inlineVsMutateLock := new(sync.RWMutex)
	callback := func(bi, bj, _ int) {
		buf := make([]uint16, maxContainerSize)
		sizeContainers, sizeKeys, keys, containers := orContainersInRangeConc(ra, bm, bi, bj, buf, inlineVsMutateLock)

		lock.Lock()
		totalSizeContainers += sizeContainers
		totalSizeKeys += sizeKeys
		allKeys = append(allKeys, keys...)
		allContainers = append(allContainers, containers...)
		lock.Unlock()
	}
	concurrentlyInRanges(numContainers, concurrency, callback)

	if totalSizeContainers > 0 {
		// ensure enough space for new containers and keys,
		// allocate required memory just once to avoid copying underlying data slice multiple times
		ra.expandNoLengthChange(totalSizeContainers + totalSizeKeys)
		ra.expandKeys(totalSizeKeys)

		for i, container := range allContainers {
			// create a new container and update the key offset to this container.
			offset := ra.newContainerNoClr(uint16(len(container)))
			copy(ra.data[offset:], container)
			ra.setKey(allKeys[i], offset)
		}
	}

	return ra
}

func orContainersInRangeConc(a, b *Bitmap, bi, bn int, buf []uint16, inlineVsMutateLock *sync.RWMutex,
) (sizeContainers, sizeKeys uint64, bKeys []uint64, bContainers [][]uint16) {
	bk := b.keys.key(bi)
	ai := a.keys.search(bk)
	an := a.keys.numKeys()

	// copy containers from b to a all at once
	// expanding underlying data slice and keys subslice once
	sizeContainers = 0
	sizeKeys = 0
	bKeys = []uint64{}
	bContainers = [][]uint16{}

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			inlineVsMutateLock.RLock()
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			c := containerOrAlt(ac, bc, buf, runInline)
			inlineVsMutateLock.RUnlock()

			if len(c) > 0 {
				inlineVsMutateLock.Lock()
				// Since buffer is used in containers merge, result container has to be copied
				// to the bitmap immediately to let buffer be reused in next merge,
				// contrary to unique containers from bitmap b copied at the end of method execution

				// Replacing previous container with merged one, that requires moving data
				// to the right to make enough space for merged container is slower
				// than appending bitmap with entirely new container and "forgetting" old one
				// for large bitmaps, so it is performed only on small ones
				if an > 10 {
					// create a new container and update the key off to this container.
					off = a.newContainerNoClr(uint16(len(c)))
					copy(a.data[off:], c)
				} else {
					// make room for container, replacing smaller one and update key offset to new container.
					off = a.keys.val(ai)
					a.insertAt(off, c)
				}
				a.setKey(ak, off)
				inlineVsMutateLock.Unlock()
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
			bKeys = append(bKeys, bk)
			bContainers = append(bContainers, bc)
			sizeContainers += uint64(len(bc))
			sizeKeys += 8 // 2x uint64 = 8x uint16; for key and offset
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
	wg.Add(concurrency)

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

		go func() {
			callback(from, to, i)
			wg.Done()
		}()
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
	containersLen := uint64(requiredContainersCount * maxContainerSize)
	keysLen := uint64(requiredContainersCount * 2 * 4)
	ra.expandNoLengthChange(containersLen + keysLen)
	ra.expandKeys(keysLen)

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
