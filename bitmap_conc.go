package sroar

import (
	"sync"
)

func (dst *Bitmap) AndConcurrently(src *Bitmap, containerBufs ...[]uint16) *Bitmap {
	assert(len(containerBufs) > 0)

	if src.IsEmpty() {
		dst.Reset()
		return dst
	}

	concurrentlyOnContainersRange(dst.keys.numKeys(), containerBufs, func(from, to int, buf []uint16) {
		andRangeContainersInline(dst, src, from, to, buf)
	})
	return dst
}

func concurrentlyOnContainersRange(numKeys int, bufs [][]uint16, callback func(from, to int, buf []uint16)) {
	concurrency := len(bufs)
	if numKeys < concurrency*minContainersForConcurrency {
		concurrency = numKeys / minContainersForConcurrency
	}
	if concurrency <= 1 {
		callback(0, numKeys, bufs[0])
		return
	}

	delta := (numKeys + concurrency - 2) / concurrency

	wg := new(sync.WaitGroup)
	wg.Add(concurrency - 1)
	for i := 0; i < concurrency-1; i++ {
		go func(i int) {
			callback(delta*i, delta*(i+1), bufs[i])
			wg.Done()
		}(i)
	}
	callback(delta*(concurrency-1), numKeys, bufs[concurrency-1])
	wg.Wait()
}

func andRangeContainersInline(a, b *Bitmap, ai, an int, buf []uint16) {
	// ak := a.keys.key(ai)
	// bi := b.keys.search(ak)
	bi := 0
	bn := b.keys.numKeys()

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)
			off = b.keys.val(bi)
			bc := b.getContainer(off)
			containerAndConc(ac, bc, buf, runInline)
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
