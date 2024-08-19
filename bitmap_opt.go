package sroar

// AndToSuperset calculates intersection of current and incoming bitmap
// It reuses containers present in current bitmap
// and utilize container buffer provided.
//
// CAUTION: should be used only when current bitmap contained before
// all elements present in incoming bitmap
func (dst *Bitmap) AndToSuperset(src *Bitmap, containerBuf []uint16) {
	if src == nil {
		for ai, an := 0, dst.keys.numKeys(); ai < an; ai++ {
			off := dst.keys.val(ai)
			zeroOutContainer(dst.getContainer(off))
		}
		return
	}

	a, b := dst, src
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

// OrToSuperset calculates union of current and incoming bitmap
// It reuses containers present in current bitmap
// and utilize container buffer provided.
//
// CAUTION: should be used only when current bitmap contained before
// all elements present in incoming bitmap
func (dst *Bitmap) OrToSuperset(src *Bitmap, containerBuf []uint16) {
	if src == nil {
		return
	}

	srcIdx, numKeys := 0, src.keys.numKeys()
	for ; srcIdx < numKeys; srcIdx++ {
		srcCont := src.getContainer(src.keys.val(srcIdx))
		if getCardinality(srcCont) == 0 {
			continue
		}

		key := src.keys.key(srcIdx)

		dstIdx := dst.keys.search(key)
		if dstIdx >= dst.keys.numKeys() || dst.keys.key(dstIdx) != key {
			// Container does not exist in dst.
			panic("Current bitmap should have all containers of incoming bitmap")
		} else {
			// Container exists in dst as well. Do an inline containerOr.
			offset := dst.keys.val(dstIdx)
			dstCont := dst.getContainer(offset)
			containerOrToSuperset(dstCont, srcCont, containerBuf)
		}
	}
}

// AndNotToSuperset calculates difference between current and incoming bitmap
// It reuses containers present in current bitmap
// and utilize container buffer provided.
//
// CAUTION: should be used only when current bitmap contained before
// all elements present in incoming bitmap
func (dst *Bitmap) AndNotToSuperset(src *Bitmap, containerBuf []uint16) {
	if src == nil {
		return
	}

	a, b := dst, src
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

			if getCardinality(bc) != 0 {
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
