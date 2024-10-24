package sroar

import (
	"math/bits"
)

func containerAndBuf(ac, bc []uint16, buf []uint16, runMode int) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andArrayBuf(right, buf, runMode)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andBitmapBuf(right, buf, runMode)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.andArrayBuf(right, buf, runMode)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andBitmapBuf(right, buf, runMode)
	}
	panic("containerAnd: We should not reach here")
}

func (c array) andArrayBuf(other array, buf []uint16, runMode int) []uint16 {
	if runMode&runInline == 0 {
		if getCardinality(c) == 0 || getCardinality(other) == 0 {
			out := buf[:minContainerSize]
			out[indexType] = typeArray
			out[indexSize] = uint16(len(out))
			setCardinality(out, 0)
			return out
		}
	} else if getCardinality(c) == 0 {
		// do nothing, array already empty
		return nil
	} else if getCardinality(other) == 0 {
		// reset array
		c.zeroOut()
		return nil
	}

	// merge
	setc := c.all()
	seto := other.all()
	num := intersection2by2(setc, seto, buf[startIdx:])
	pos := startIdx + uint16(num)

	if runMode&runInline == 0 {
		out := buf[:max(int(pos), minContainerSize)]
		out[indexType] = typeArray
		out[indexSize] = uint16(len(out))
		setCardinality(out, num)
		return out
	}
	setCardinality(c, num)
	copy(c[startIdx:], buf[startIdx:pos])
	return nil
}

func (c array) andBitmapBuf(other bitmap, buf []uint16, runMode int) []uint16 {
	if runMode&runInline == 0 {
		if getCardinality(c) == 0 || getCardinality(other) == 0 {
			out := buf[:minContainerSize]
			out[indexType] = typeArray
			out[indexSize] = uint16(len(out))
			setCardinality(out, 0)
			return out
		}
	} else if getCardinality(c) == 0 {
		// do nothing, array already empty
		return nil
	} else if getCardinality(other) == 0 {
		// reset array
		c.zeroOut()
		return nil
	}

	// merge
	pos := startIdx
	for _, x := range c.all() {
		if other.bitValue(x) > 0 {
			buf[pos] = x
			pos++
		}
	}
	num := int(pos - startIdx)

	if runMode&runInline == 0 {
		out := buf[:max(int(pos), minContainerSize)]
		out[indexType] = typeArray
		out[indexSize] = uint16(len(out))
		setCardinality(out, num)
		return out
	}
	setCardinality(c, num)
	copy(c[startIdx:], buf[startIdx:pos])
	return nil
}

func (b bitmap) andArrayBuf(other array, buf []uint16, runMode int) []uint16 {
	if runMode&runInline > 0 {
		if bnum := getCardinality(b); bnum == 0 {
			// do nothing, bitmap already empty
		} else if onum := getCardinality(other); onum == 0 {
			// reset bitmap
			b.zeroOut()
		} else {
			// convert array to bitmap
			copy(buf, zeroContainer)
			for _, x := range other.all() {
				idx := x >> 4
				pos := x & 0xF
				buf[startIdx+idx] |= bitmapMask[pos]
			}
			// merge
			b64 := uint16To64Slice(b[startIdx:])
			o64 := uint16To64Slice(buf[startIdx:])
			var num int
			for i := range b64 {
				b64[i] &= o64[i]
				num += bits.OnesCount64(b64[i])
			}
			setCardinality(b, num)
		}
		return nil
	}

	num := 0
	size := minContainerSize

	if getCardinality(b) > 0 && getCardinality(other) > 0 {
		pos := startIdx
		for _, x := range other.all() {
			if b.bitValue(x) > 0 {
				buf[pos] = x
				pos++
			}
		}
		num = int(pos - startIdx)
		size = max(int(pos), minContainerSize)
	}

	out := buf[:size]
	out[indexType] = typeArray
	out[indexSize] = uint16(len(out))
	setCardinality(out, num)
	return out
}

func (b bitmap) andBitmapBuf(other bitmap, buf []uint16, runMode int) []uint16 {
	if runMode&runInline > 0 {
		buf = b
	} else {
		copy(buf, b)
	}

	if bnum := getCardinality(buf); bnum == 0 {
		// do nothing, bitmap already empty
	} else if onum := getCardinality(other); onum == 0 {
		// reset bitmap
		bitmap(buf).zeroOut()
	} else {
		// merge
		b64 := uint16To64Slice(buf[startIdx:])
		o64 := uint16To64Slice(other[startIdx:])
		var num int
		for i := range b64 {
			b64[i] &= o64[i]
			num += bits.OnesCount64(b64[i])
		}
		setCardinality(buf, num)
	}

	if runMode&runInline > 0 {
		return nil
	}
	return buf
}

func containerAndAlt(ac, bc []uint16, runMode int) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andArrayAlt(right, runMode)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andBitmapAlt(right, runMode)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.andArrayAlt(right, runMode)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andBitmapAlt(right, runMode)
	}
	panic("containerAnd: We should not reach here")
}

func (c array) andArrayAlt(other array, runMode int) []uint16 {
	cnum := getCardinality(c)
	onum := getCardinality(other)

	if cnum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// reset array
		c.zeroOut()
		return nil
	}

	// merge
	min := min(cnum, onum)
	size := max(min+int(startIdx), minContainerSize)
	out := make([]uint16, size)
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

func (c array) andBitmapAlt(other bitmap, runMode int) []uint16 {
	cnum := getCardinality(c)
	onum := getCardinality(other)

	if cnum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// reset array
		c.zeroOut()
		return nil
	}

	// merge
	min := min(cnum, onum)
	size := max(int(startIdx)+min, minContainerSize)
	out := make([]uint16, size)
	lastIdx := startIdx
	for _, x := range c.all() {
		if other.bitValue(x) > 0 {
			out[lastIdx] = x
			lastIdx++
		}
	}
	num := int(lastIdx - startIdx)

	if runMode&runInline == 0 {
		return bufAsArray(out, lastIdx)
	}
	setCardinality(c, num)
	copy(c[startIdx:], out[startIdx:lastIdx])
	return nil
}

func (b bitmap) andArrayAlt(other array, runMode int) []uint16 {
	bnum := getCardinality(b)
	onum := getCardinality(other)

	if bnum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// reset array
		b.zeroOut()
		return nil
	}

	// merge
	if runMode&runInline == 0 || onum < 1024 {
		size := max(onum+int(startIdx), minContainerSize)
		out := make([]uint16, size)
		lastIdx := startIdx
		for _, x := range other.all() {
			if b.bitValue(x) > 0 {
				out[lastIdx] = x
				lastIdx++
			}
		}

		if runMode&runInline == 0 {
			return bufAsArray(out, lastIdx)
		}
		b[indexType] = typeArray
		setCardinality(b, int(lastIdx-startIdx))
		copy(b[startIdx:], out[startIdx:lastIdx])
		return nil
	}

	buf := make([]uint16, maxContainerSize)
	for _, x := range other.all() {
		idx := x >> 4
		pos := x & 0xF
		buf[startIdx+idx] |= bitmapMask[pos]
	}
	b64 := uint16To64Slice(b[startIdx:])
	o64 := uint16To64Slice(buf[startIdx:])
	var num int
	for i := range b64 {
		b64[i] &= o64[i]
		num += bits.OnesCount64(b64[i])
	}
	setCardinality(b, num)
	return nil
}

func (b bitmap) andBitmapAlt(other bitmap, runMode int) []uint16 {
	bnum := getCardinality(b)
	onum := getCardinality(other)

	if bnum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// reset bitmap
		bitmap(b).zeroOut()
		return nil
	}

	// merge
	out := b
	if runMode&runInline == 0 {
		out = make([]uint16, maxContainerSize)
		copy(out, b)
	}

	b64 := uint16To64Slice(out[startIdx:])
	o64 := uint16To64Slice(other[startIdx:])
	var num int
	for i := range b64 {
		b64[i] &= o64[i]
		num += bits.OnesCount64(b64[i])
	}
	setCardinality(out, num)

	if runMode&runInline == 0 {
		return out
	}
	return nil
}

func containerAndNotAlt(ac, bc []uint16, runMode int) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andNotArrayAlt(right, runMode)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andNotBitmapAlt(right, runMode)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.andNotArrayAlt(right, runMode)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andNotBitmapAlt(right, runMode)
	}
	panic("containerAnd: We should not reach here")
}

func (c array) andNotArrayAlt(other array, runMode int) []uint16 {
	cnum := getCardinality(c)
	onum := getCardinality(other)

	if cnum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return copyArray(c)
		}
		// do nothing, nothing to remove
		return nil
	}

	// merge
	size := max(int(startIdx)+cnum, minContainerSize)
	out := make([]uint16, size)
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

func (c array) andNotBitmapAlt(other bitmap, runMode int) []uint16 {
	cnum := getCardinality(c)
	onum := getCardinality(other)

	if cnum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return copyArray(c)
		}
		// do nothing, nothing to remove
		return nil
	}

	// merge
	size := max(int(startIdx)+cnum, minContainerSize)
	out := make([]uint16, size)
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
	copy(c[startIdx:], out[startIdx:lastIdx])
	return nil
}

func (b bitmap) andNotArrayAlt(other array, runMode int) []uint16 {
	bnum := getCardinality(b)
	onum := getCardinality(other)

	if bnum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return copyBitmap(b)
		}
		// do nothing, nothing to remove
		return nil
	}

	// merge
	out := b
	if runMode&runInline == 0 {
		out = copyBitmap(b)
	}

	delnum := 0
	for _, x := range other.all() {
		idx := x >> 4
		pos := x & 0xF
		if has := out[startIdx+idx] & bitmapMask[pos]; has > 0 {
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

func (b bitmap) andNotBitmapAlt(other bitmap, runMode int) []uint16 {
	bnum := getCardinality(b)
	onum := getCardinality(other)

	if bnum == 0 {
		if runMode&runInline == 0 {
			return emptyArray()
		}
		// do nothing, array already empty
		return nil
	}
	if onum == 0 {
		if runMode&runInline == 0 {
			return copyBitmap(b)
		}
		// do nothing, nothing to remove
		return nil
	}

	// merge
	out := b
	if runMode&runInline == 0 {
		out = copyBitmap(b)
	}

	b64 := uint16To64Slice(out[startIdx:])
	o64 := uint16To64Slice(other[startIdx:])
	var num int
	for i := range b64 {
		b64[i] &^= o64[i]
		num += bits.OnesCount64(b64[i])
	}
	setCardinality(out, num)

	if runMode&runInline == 0 {
		return out
	}
	return nil
}

func emptyArray() []uint16 {
	out := make([]uint16, minContainerSize)
	out[indexType] = typeArray
	out[indexSize] = uint16(len(out))
	setCardinality(out, 0)
	return out
}

func copyArray(c array) []uint16 {
	cnum := getCardinality(c)
	lastIdx := startIdx + uint16(cnum)
	size := max(int(lastIdx), minContainerSize)

	out := make([]uint16, size)
	out[indexType] = typeArray
	out[indexSize] = uint16(len(out))
	setCardinality(out, cnum)
	copy(out[startIdx:], c[startIdx:lastIdx])
	return out
}

func copyBitmap(b bitmap) []uint16 {
	out := make([]uint16, maxContainerSize)
	copy(out, b)
	return out
}

func bufAsArray(buf []uint16, lastIdx uint16) []uint16 {
	out := buf[:max(int(lastIdx), minContainerSize)]
	out[indexType] = typeArray
	out[indexSize] = uint16(len(out))
	setCardinality(out, int(lastIdx-startIdx))
	return out
}
