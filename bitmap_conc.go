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
			if c := containerAndAlt(ac, bc, buf, runMode); len(c) > 0 {
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
	return ra.AndBuf(bm, make([]uint16, maxContainerSize))
}

func (ra *Bitmap) AndBuf(bm *Bitmap, buf []uint16) *Bitmap {
	if bm.IsEmpty() {
		ra.Reset()
		return ra
	}

	andContainersInRange(ra, bm, 0, ra.keys.numKeys(), buf, runInline)
	return ra
}
