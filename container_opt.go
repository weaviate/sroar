package sroar

import "math/bits"

var emptyArrayContainer []uint16

func init() {
	emptyArrayContainer = make([]uint16, minContainerSize)
	emptyArrayContainer[indexType] = typeArray
	emptyArrayContainer[indexSize] = uint16(len(emptyArrayContainer))
	setCardinality(emptyArrayContainer, 0)
}

func containerAndAlt(ac, bc []uint16, optBuf []uint16, runMode int) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andArrayAlt(right, optBuf, runMode)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andBitmapAlt(right, optBuf, runMode)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.andArrayAlt(right, optBuf, runMode)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andBitmapAlt(right, optBuf, runMode)
	}
	panic("containerAnd: We should not reach here")
}

func (c array) andArrayAlt(other array, optBuf []uint16, runMode int) []uint16 {
	cnum := getCardinality(c)
	onum := getCardinality(other)

	if cnum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// reset array
		c.zeroOut()
		return nil
	}

	// merge
	out := optBuf
	if out == nil {
		min := min(cnum, onum)
		out = make([]uint16, roundSize(startIdx+uint16(min)))
	}
	setc := c.all()
	seto := other.all()
	num := intersection2by2(setc, seto, out[startIdx:])
	lastIdx := startIdx + uint16(num)

	if runMode&runInline == 0 {
		return bufAsArray(out, lastIdx)
	}
	setCardinality(c, num)
	copy(c[startIdx:], out[startIdx:lastIdx])
	return nil
}

func (c array) andBitmapAlt(other bitmap, optBuf []uint16, runMode int) []uint16 {
	cnum := getCardinality(c)
	onum := getCardinality(other)

	if cnum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// reset array
		c.zeroOut()
		return nil
	}

	// merge
	out := c
	if runMode&runInline == 0 {
		out = optBuf
		if out == nil {
			min := min(cnum, onum)
			out = make([]uint16, roundSize(startIdx+uint16(min)))
		}
	}
	lastIdx := startIdx
	for _, x := range c.all() {
		if other.has(x) {
			out[lastIdx] = x
			lastIdx++
		}
	}

	if runMode&runInline == 0 {
		return bufAsArray(out, lastIdx)
	}
	setCardinality(c, int(lastIdx-startIdx))
	return nil
}

func (b bitmap) andArrayAlt(other array, optBuf []uint16, runMode int) []uint16 {
	bnum := getCardinality(b)
	onum := getCardinality(other)

	if bnum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// do nothing, bitmap already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// reset bitmap
		b.zeroOut()
		return nil
	}

	// merge
	if runMode&runInline == 0 {
		out := optBuf
		if out == nil {
			out = make([]uint16, roundSize(startIdx+uint16(onum)))
		}
		lastIdx := startIdx
		for _, x := range other.all() {
			if b.has(x) {
				out[lastIdx] = x
				lastIdx++
			}
		}

		return bufAsArray(out, lastIdx)
	}

	// if no buffer, create small buffer and perform merge in batches
	if optBuf == nil {
		batches := uint16(8)
		bufSize := (maxContainerSize - startIdx) / batches
		buf := make([]uint16, bufSize)
		buf64 := uint16To64SliceUnsafe(buf)
		dst64 := uint16To64SliceUnsafe(b[startIdx:])

		merge := func(from, to uint16) int {
			num := 0
			delta := int(from * bufSize / 4)
			for i := range buf64 {
				dst64[i+delta] &= buf64[i]
				buf64[i] = 0
				num += bits.OnesCount64(dst64[i+delta])
			}
			for b := from + 1; b < to; b++ {
				delta := int(b * bufSize / 4)
				for i := range buf64 {
					dst64[i+delta] = 0
				}
			}
			return num
		}

		num := 0
		lastBatch := uint16(0)
		for _, x := range other.all() {
			idx := x >> 4
			pos := x & 0xF

			batch := idx / bufSize
			if batch != lastBatch {
				num += merge(lastBatch, batch)
				lastBatch = batch
			}

			buf[idx-batch*bufSize] |= bitmapMask[pos]
		}
		num += merge(lastBatch, batches)
		setCardinality(b, num)
		return nil
	}

	copy(optBuf, zeroContainer)
	for _, x := range other.all() {
		idx := x >> 4
		pos := x & 0xF
		optBuf[startIdx+idx] |= bitmapMask[pos]
	}

	dst64 := uint16To64SliceUnsafe(b[startIdx:])
	src64 := uint16To64SliceUnsafe(optBuf[startIdx:])
	var num int
	for i := range dst64 {
		dst64[i] &= src64[i]
		num += bits.OnesCount64(dst64[i])
	}
	setCardinality(b, num)
	return nil
}

func (b bitmap) andBitmapAlt(other bitmap, optBuf []uint16, runMode int) []uint16 {
	bnum := getCardinality(b)
	onum := getCardinality(other)

	if bnum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// reset bitmap
		b.zeroOut()
		return nil
	}

	// merge
	out := b
	if runMode&runInline == 0 {
		out = copyBitmap(b, optBuf)
	}

	dst64 := uint16To64SliceUnsafe(out[startIdx:])
	src64 := uint16To64SliceUnsafe(other[startIdx:])
	var num int
	for i := range dst64 {
		dst64[i] &= src64[i]
		num += bits.OnesCount64(dst64[i])
	}
	setCardinality(out, num)

	if runMode&runInline == 0 {
		return out
	}
	return nil
}

func containerAndNotAlt(ac, bc []uint16, optBuf []uint16, runMode int) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andNotArrayAlt(right, optBuf, runMode)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andNotBitmapAlt(right, optBuf, runMode)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.andNotArrayAlt(right, optBuf, runMode)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andNotBitmapAlt(right, optBuf, runMode)
	}
	panic("containerAnd: We should not reach here")
}

func (c array) andNotArrayAlt(other array, optBuf []uint16, runMode int) []uint16 {
	cnum := getCardinality(c)
	onum := getCardinality(other)

	if cnum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return resizeArray(c, optBuf)
		}
		// do nothing, nothing to remove
		return nil
	}

	// merge
	out := optBuf
	if out == nil {
		out = make([]uint16, roundSize(startIdx+uint16(cnum)))
	}
	setc := c.all()
	seto := other.all()
	num := difference(setc, seto, out[startIdx:])
	lastIdx := startIdx + uint16(num)

	if runMode&runInline == 0 {
		return bufAsArray(out, lastIdx)
	}
	setCardinality(c, num)
	copy(c[startIdx:], out[startIdx:lastIdx])
	return nil
}

func (c array) andNotBitmapAlt(other bitmap, optBuf []uint16, runMode int) []uint16 {
	cnum := getCardinality(c)
	onum := getCardinality(other)

	if cnum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return resizeArray(c, optBuf)
		}
		// do nothing, nothing to remove
		return nil
	}

	// merge
	out := c
	if runMode&runInline == 0 {
		out = optBuf
		if out == nil {
			out = make([]uint16, roundSize(startIdx+uint16(cnum)))
		}
	}

	lastIdx := startIdx
	for _, x := range c.all() {
		if !other.has(x) {
			out[lastIdx] = x
			lastIdx++
		}
	}

	if runMode&runInline == 0 {
		return bufAsArray(out, lastIdx)
	}
	setCardinality(c, int(lastIdx-startIdx))
	return nil
}

func (b bitmap) andNotArrayAlt(other array, optBuf []uint16, runMode int) []uint16 {
	bnum := getCardinality(b)
	onum := getCardinality(other)

	if bnum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return b
		}
		// do nothing, nothing to remove
		return nil
	}

	// merge
	out := b
	if runMode&runInline == 0 {
		out = copyBitmap(b, optBuf)
	}

	delnum := 0
	for _, x := range other.all() {
		idx := x >> 4
		pos := x & 0xF
		if has := out[startIdx+idx]&bitmapMask[pos] > 0; has {
			out[startIdx+idx] ^= bitmapMask[pos]
			delnum++
		}
	}
	setCardinality(out, bnum-delnum)

	if runMode&runInline == 0 {
		return out
	}
	return nil
}

func (b bitmap) andNotBitmapAlt(other bitmap, optBuf []uint16, runMode int) []uint16 {
	bnum := getCardinality(b)
	onum := getCardinality(other)

	if bnum == 0 {
		if runMode&runInline == 0 {
			return emptyArrayContainer
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return b
		}
		// do nothing, nothing to remove
		return nil
	}

	// merge
	out := b
	if runMode&runInline == 0 {
		out = copyBitmap(b, optBuf)
	}

	dst64 := uint16To64SliceUnsafe(out[startIdx:])
	src64 := uint16To64SliceUnsafe(other[startIdx:])
	var num int
	for i := range dst64 {
		dst64[i] &^= src64[i]
		num += bits.OnesCount64(dst64[i])
	}
	setCardinality(out, num)

	if runMode&runInline == 0 {
		return out
	}
	return nil
}

func containerOrAlt(ac, bc []uint16, buf []uint16, runMode int) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.orArrayAlt(right, buf, runMode)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.orBitmapAlt(right, buf, runMode)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.orArrayAlt(right, buf, runMode)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.orBitmapAlt(right, buf, runMode)
	}
	panic("containerOr: We should not reach here")
}

func (c array) orArrayAlt(other array, buf []uint16, runMode int) []uint16 {
	cnum := getCardinality(c)
	onum := getCardinality(other)

	if onum == 0 {
		if runMode&runInline == 0 {
			return resizeArray(c, buf)
		}
		// do nothing, nothing to add
		return nil
	}
	if cnum == 0 {
		if runMode&runInline == 0 {
			return resizeArray(other, buf)
		}
		// overwrite array or return if does not fit
		lastIdx := startIdx + uint16(onum)
		if c[indexSize] < lastIdx {
			return resizeArray(other, buf)
		}
		setCardinality(c, onum)
		copy(c[startIdx:], other[startIdx:lastIdx])
		return nil
	}

	// merge
	out := buf
	sum := cnum + onum
	size := startIdx + uint16(sum)
	// if merged arrays may exceed max container size convert to bitmap
	if size >= maxContainerSize/5*3 {
		copy(out, zeroContainer)
		out[indexType] = typeBitmap
		out[indexSize] = maxContainerSize

		smaller, larger := other, c
		if onum > cnum {
			smaller, larger = c, other
		}

		var num int
		for _, x := range larger.all() {
			idx := x >> 4
			pos := x & 0xF
			out[startIdx+idx] |= bitmapMask[pos]
			num++
		}
		for _, x := range smaller.all() {
			idx := x >> 4
			pos := x & 0xF
			if has := out[startIdx+idx]&bitmapMask[pos] > 0; !has {
				out[startIdx+idx] |= bitmapMask[pos]
				num++
			}
		}
		setCardinality(out, num)

		if runMode&runInline == 0 {
			return out
		}
		if c[indexSize] < maxContainerSize {
			return out
		}
		copy(c, out)
		return nil
	}

	num := union2by2(c.all(), other.all(), out[startIdx:])
	lastIdx := startIdx + uint16(num)

	if runMode&runInline == 0 {
		return bufAsArray(out, lastIdx)
	}
	if c[indexSize] < lastIdx {
		return bufAsArray(out, lastIdx)
	}
	setCardinality(c, num)
	copy(c[startIdx:], out[startIdx:lastIdx])
	return nil
}

func (c array) orBitmapAlt(other bitmap, buf []uint16, runMode int) []uint16 {
	cnum := getCardinality(c)
	onum := getCardinality(other)

	if onum == 0 {
		if runMode&runInline == 0 {
			return resizeArray(c, buf)
		}
		// do nothing, nothing to add
		return nil
	}
	if cnum == 0 || onum == maxCardinality {
		if runMode&runInline == 0 {
			return other
		}
		// overwrite converting to bitmap or return bitmap if does not fit
		if c[indexSize] != maxContainerSize {
			return other
		}
		copy(c, other)
		return nil
	}

	// merge
	out := buf
	copy(out, zeroContainer)
	out[indexType] = typeBitmap
	out[indexSize] = maxContainerSize
	for _, x := range c.all() {
		idx := x >> 4
		pos := x & 0xF
		out[startIdx+idx] |= bitmapMask[pos]
	}

	dst64 := uint16To64SliceUnsafe(out[startIdx:])
	src64 := uint16To64SliceUnsafe(other[startIdx:])
	var num int
	for i := range dst64 {
		dst64[i] |= src64[i]
		num += bits.OnesCount64(dst64[i])
	}
	setCardinality(out, num)

	if runMode&runInline == 0 {
		return out
	}
	if c[indexSize] != maxContainerSize {
		return out
	}
	copy(c, out)
	return nil
}

func (b bitmap) orArrayAlt(other array, buf []uint16, runMode int) []uint16 {
	bnum := getCardinality(b)
	onum := getCardinality(other)

	if onum == 0 || bnum == maxCardinality {
		if runMode&runInline == 0 {
			return b
		}
		// do nothing, nothing to add
		return nil
	}
	if bnum == 0 {
		if runMode&runInline == 0 {
			return resizeArray(other, buf)
		}
		// proceed to merge
	}

	// merge
	out := b
	if runMode&runInline == 0 {
		out = buf
		copy(out, b)
	}

	addnum := 0
	for _, x := range other.all() {
		idx := x >> 4
		pos := x & 0xF
		if has := out[startIdx+idx]&bitmapMask[pos] > 0; !has {
			out[startIdx+idx] |= bitmapMask[pos]
			addnum++
		}
	}
	setCardinality(out, bnum+addnum)

	if runMode&runInline == 0 {
		return out
	}
	return nil
}

func (b bitmap) orBitmapAlt(other bitmap, buf []uint16, runMode int) []uint16 {
	bnum := getCardinality(b)
	onum := getCardinality(other)

	if onum == 0 || bnum == maxCardinality {
		if runMode&runInline == 0 {
			return b
		}
		// do nothing, nothing to add
		return nil
	}
	if bnum == 0 || onum == maxCardinality {
		if runMode&runInline == 0 {
			return other
		}
		// overwrite bitmap
		copy(b, other)
		return nil
	}

	// merge
	out := b
	if runMode&runInline == 0 {
		out = buf
		copy(out, b)
	}

	dst64 := uint16To64SliceUnsafe(out[startIdx:])
	src64 := uint16To64SliceUnsafe(other[startIdx:])
	var num int
	for i := range dst64 {
		dst64[i] |= src64[i]
		num += bits.OnesCount64(dst64[i])
	}
	setCardinality(out, num)

	if runMode&runInline == 0 {
		return out
	}
	return nil
}

func resizeArray(c array, out []uint16) []uint16 {
	csize := c[indexSize]
	cnum := getCardinality(c)
	lastIdx := startIdx + uint16(cnum)
	size := roundSize(lastIdx)

	if size == csize {
		return c
	}

	if out == nil {
		out = make([]uint16, size)
	} else {
		out = out[:size]
	}
	out[indexType] = typeArray
	out[indexSize] = uint16(len(out))
	setCardinality(out, cnum)
	copy(out[startIdx:], c[startIdx:lastIdx])
	return out
}

func copyBitmap(b bitmap, out []uint16) []uint16 {
	if out == nil {
		out = make([]uint16, maxContainerSize)
	}
	copy(out, b)
	return out
}

func bufAsArray(buf []uint16, lastIdx uint16) []uint16 {
	out := buf[:roundSize(lastIdx)]
	out[indexType] = typeArray
	out[indexSize] = uint16(len(out))
	setCardinality(out, int(lastIdx-startIdx))
	return out
}

func roundSize(size uint16) uint16 {
	// <=64 -> 64
	// <=128 -> 128
	// <=256 -> 256
	// <=512 -> 512
	// <=1024 -> 1024
	// <=2048 -> 2048
	//  >2048 -> maxSize
	for i := uint16(64); i <= 2048; i *= 2 {
		if size <= i {
			return i
		}
	}
	return maxContainerSize
}
