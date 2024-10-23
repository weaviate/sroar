package sroar

import (
	"math/bits"
)

func containerAndAlt(ac, bc []uint16, buf []uint16, runMode int) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andArrayAlt(right, buf, runMode)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andBitmapAlt(right, buf, runMode)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.andArrayAlt(right, buf, runMode)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andBitmapAlt(right, buf, runMode)
	}
	panic("containerAnd: We should not reach here")
}

func (c array) andArrayAlt(other array, buf []uint16, runMode int) []uint16 {
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

func (c array) andBitmapAlt(other bitmap, buf []uint16, runMode int) []uint16 {
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

func (b bitmap) andArrayAlt(other array, buf []uint16, runMode int) []uint16 {
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

func (b bitmap) andBitmapAlt(other bitmap, buf []uint16, runMode int) []uint16 {
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
