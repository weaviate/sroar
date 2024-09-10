package sroar

import (
	"sync"
)

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
