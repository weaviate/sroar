package sroar

import (
	"sync"
)

func (dst *Bitmap) AndConcurrently(src *Bitmap, containerBufs ...[]uint16) *Bitmap {
	assert(len(containerBufs) > 0)

	if src == nil {
		dst.Reset()
		return dst
	}

	concurrentlyOnContainersRange(dst.keys.numKeys(), containerBufs, func(from, to int, buf []uint16) {
		andContainersInRange(dst, src, from, to, buf, runInline)
	})
	return dst
}

func concurrentlyOnContainersRange(numKeys int, bufs [][]uint16, callback func(from, to int, buf []uint16)) {
	concurrency := len(bufs)
	if concurrency > 1 && numKeys < concurrency*minContainersForConcurrency {
		concurrency = numKeys / minContainersForConcurrency
	}

	if concurrency <= 1 {
		callback(0, numKeys, bufs[0])
		return
	}

	wg := new(sync.WaitGroup)
	wg.Add(concurrency - 1)

	delta := numKeys / concurrency
	rem := numKeys - delta*concurrency
	from := 0
	for i := 0; i < rem; i++ {
		to := from + delta + 1
		go func(i, from int) {
			callback(from, to, bufs[i])
			wg.Done()
		}(i, from)
		from = to
	}
	for i := rem; i < concurrency-1; i++ {
		to := from + delta
		go func(i, from int) {
			callback(from, to, bufs[i])
			wg.Done()
		}(i, from)
		from = to
	}
	callback(from, numKeys, bufs[concurrency-1])
	wg.Wait()
}

func (ra *Bitmap) AndBuf(bm *Bitmap, buf []uint16) *Bitmap {
	if bm.IsEmpty() {
		ra.Reset()
		return ra
	}

	andContainersInRange(ra, bm, 0, ra.keys.numKeys(), buf, runInline)
	return ra
}

func andContainersInRange(a, b *Bitmap, ai, an int, buf []uint16, runMode int) {
	ak := a.keys.key(ai)
	bi := b.keys.search(ak)
	bn := b.keys.numKeys()

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			if c := containerAndBuf(ac, bc, buf, runMode); len(c) > 0 {
				// create a new container and update the key offset to this container.
				offset := a.newContainer(uint16(len(c)))
				copy(a.data[offset:], c)
				a.setKey(ak, offset)
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

func (ra *Bitmap) AndAlt(bm *Bitmap) *Bitmap {
	if bm.IsEmpty() {
		ra.Reset()
		return ra
	}

	andContainersInRangeAlt(ra, bm, 0, ra.keys.numKeys(), runInline)
	return ra
}

func andContainersInRangeAlt(a, b *Bitmap, ai, an int, runMode int) {
	ak := a.keys.key(ai)
	bi := b.keys.search(ak)
	bn := b.keys.numKeys()

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			if c := containerAndAlt(ac, bc, runMode); len(c) > 0 {
				// create a new container and update the key offset to this container.
				offset := a.newContainer(uint16(len(c)))
				copy(a.data[offset:], c)
				a.setKey(ak, offset)
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

func AndAlt(a, b *Bitmap) *Bitmap {
	res := NewBitmap()

	if a.IsEmpty() || b.IsEmpty() {
		return res
	}

	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := a.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			if c := containerAndAlt(ac, bc, 0); len(c) > 0 {
				// create a new container and update the key offset to this container.
				offset := res.newContainer(uint16(len(c)))
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

	return res
}

func (ra *Bitmap) AndNotAlt(bm *Bitmap) *Bitmap {
	if bm.IsEmpty() {
		return ra
	}

	andNotContainersInRangeAlt(ra, bm, 0, ra.keys.numKeys(), runInline)
	return ra
}

func andNotContainersInRangeAlt(a, b *Bitmap, ai, an int, runMode int) {
	ak := a.keys.key(ai)
	bi := b.keys.search(ak)
	bn := b.keys.numKeys()

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			if c := containerAndNotAlt(ac, bc, runMode); len(c) > 0 {
				// create a new container and update the key offset to this container.
				offset := a.newContainer(uint16(len(c)))
				copy(a.data[offset:], c)
				a.setKey(ak, offset)
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

func AndNotAlt(a, b *Bitmap) *Bitmap {
	res := NewBitmap()

	if a.IsEmpty() {
		return res
	}
	if b.IsEmpty() {
		return a.Clone()
	}

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
			if c := containerAndNotAlt(ac, bc, 0); len(c) > 0 && getCardinality(c) > 0 {
				// create a new container and update the key offset to this container.
				offset := res.newContainer(uint16(len(c)))
				copy(res.data[offset:], c)
				res.setKey(ak, offset)
			}
			ai++
			bi++
		} else if ak < bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			if getCardinality(ac) > 0 {
				offset := res.newContainer(uint16(len(ac)))
				copy(res.data[offset:], ac)
				res.setKey(ak, offset)
			}
			ai++
		} else {
			bi++
		}
	}
	for ; ai < an; ai++ {
		off := a.keys.val(ai)
		ac := a.getContainer(off)
		if getCardinality(ac) > 0 {
			ak := a.keys.key(ai)
			off = res.newContainer(uint16(len(ac)))
			copy(res.data[off:], ac)
			res.setKey(ak, off)
		}
	}

	return res
}
