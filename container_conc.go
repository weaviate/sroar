package sroar

import "math/bits"

func containerAndConc(ac, bc []uint16, buf []uint16, runMode int) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andArrayConc(right, buf, runMode)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andBitmapConc(right, buf, runMode)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.andArrayConc(right, buf, runMode)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andBitmapConc(right, buf, runMode)
	}
	panic("containerAnd: We should not reach here")
}

func (c array) andArrayConc(other array, buf []uint16, runMode int) []uint16 {
	if cnum := getCardinality(c); cnum == 0 {
		if runMode&runInline == 0 {
			out := buf[:minContainerSize]
			out[indexType] = typeArray
			out[indexSize] = uint16(len(out))
			setCardinality(out, 0)
			return out
		}
		// do nothing, array already empty
		return nil
	}
	if onum := getCardinality(other); onum == 0 {
		if runMode&runInline == 0 {
			out := buf[:minContainerSize]
			out[indexType] = typeArray
			out[indexSize] = uint16(len(out))
			setCardinality(out, 0)
			return out
		}
		// reset array
		c.zeroOut()
		return nil
	}

	// merge
	setc := c.all()
	seto := other.all()
	num := intersection2by2(setc, seto, buf[startIdx:])
	pos := startIdx + uint16(num)
	size := max(int(pos), minContainerSize)

	if runMode&runInline == 0 {
		out := buf[:size]
		out[indexType] = typeArray
		out[indexSize] = uint16(len(out))
		setCardinality(out, num)
		return out
	}
	setCardinality(c, num)
	copy(c[startIdx:], buf[startIdx:pos])
	return nil
}

func (c array) andBitmapConc(other bitmap, buf []uint16, runMode int) []uint16 {
	if cnum := getCardinality(c); cnum == 0 {
		if runMode&runInline == 0 {
			out := buf[:minContainerSize]
			out[indexType] = typeArray
			out[indexSize] = uint16(len(out))
			setCardinality(out, 0)
			return out
		}
		// do nothing, array already empty
		return nil
	}
	if onum := getCardinality(other); onum == 0 {
		if runMode&runInline == 0 {
			out := buf[:minContainerSize]
			out[indexType] = typeArray
			out[indexSize] = uint16(len(out))
			setCardinality(out, 0)
			return out
		}
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
	size := max(int(pos), minContainerSize)

	if runMode&runInline == 0 {
		out := buf[:size]
		out[indexType] = typeArray
		out[indexSize] = uint16(len(out))
		setCardinality(out, num)
		return out
	}
	setCardinality(c, num)
	copy(c[startIdx:], buf[startIdx:pos])
	return nil
}

func (b bitmap) andArrayConc(other array, buf []uint16, runMode int) []uint16 {
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
		for _, x := range other.all() {
			idx := x / 16
			pos := x % 16

			val := &buf[startIdx+idx]
			before := bits.OnesCount16(*val)
			*val &= bitmapMask[pos]
			after := bits.OnesCount16(*val)
			bnum -= before - after
		}
		setCardinality(buf, bnum)
	}

	if runMode&runInline > 0 {
		return nil
	}
	return buf
}

func (b bitmap) andBitmapConc(other bitmap, buf []uint16, runMode int) []uint16 {
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
