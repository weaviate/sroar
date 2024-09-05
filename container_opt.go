package sroar

import "math/bits"

func containerAndToSuperset(ac, bc, buf []uint16) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andArrayToSuperset(right, buf)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.andArrayToSuperset(right, buf)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andBitmapToSuperset(right)
	}
	panic("containerAndToSuperset: We should not reach here")
}

func containerOrToSuperset(ac, bc, buf []uint16) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.orArrayToSuperset(right, buf)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.orArray(right, buf, runInline)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.orBitmapToSuperset(right)
	}
	panic("containerOrToSuperset: We should not reach here")
}

func containerAndNotToSuperset(ac, bc, buf []uint16) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andNotArrayToSuperset(right, buf)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		out := left.andNotArray(right)
		return out
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andNotBitmapToSuperset(right)
	}
	panic("containerAndNotToSuperset: We should not reach here")
}

func (a array) andArrayToSuperset(other array, buf []uint16) []uint16 {
	copy(buf, zeroContainer)
	out := buf[:len(a)]

	num := intersection2by2(a.all(), other.all(), out[startIdx:])
	setCardinality(out, num)
	copy(a[2:], out[2:])
	return a
}

func (a array) orArrayToSuperset(other array, buf []uint16) []uint16 {
	copy(buf, zeroContainer)
	out := buf[:len(a)]

	num := union2by2(a.all(), other.all(), out[startIdx:])
	setCardinality(out, num)
	copy(a[2:], out[2:])
	return a
}

func (a array) andNotArrayToSuperset(other array, buf []uint16) []uint16 {
	copy(buf, zeroContainer)
	out := buf[:len(a)]

	andRes := array(a.andArray(other)).all() // TODO is andRes needed?
	num := difference(a.all(), andRes, out[startIdx:])
	setCardinality(out, int(num))
	copy(a[2:], out[2:])
	return a
}

func (b bitmap) andBitmapToSuperset(other bitmap) []uint16 {
	b64 := uint16To64Slice(b[startIdx:])
	o64 := uint16To64Slice(other[startIdx:])

	var num int
	for i := range b64 {
		b64[i] &= o64[i]
		num += bits.OnesCount64(b64[i])
	}
	setCardinality(b, num)
	return b
}

func (b bitmap) orBitmapToSuperset(other bitmap) []uint16 {
	if num := getCardinality(b); num == maxCardinality {
		// do nothing. bitmap is already full.
		return b
	}

	b64 := uint16To64Slice(b[startIdx:])
	o64 := uint16To64Slice(other[startIdx:])

	var num int
	for i := range b64 {
		b64[i] |= o64[i]
		num += bits.OnesCount64(b64[i])
	}
	setCardinality(b, num)
	return b
}

func (b bitmap) andArrayToSuperset(other array, buf []uint16) []uint16 {
	otherb := other.toBitmapContainer(buf)
	return b.andBitmapToSuperset(otherb)
}

func (b bitmap) andNotBitmapToSuperset(other bitmap) []uint16 {
	b64 := uint16To64Slice(b[startIdx:])
	o64 := uint16To64Slice(other[startIdx:])

	var num int
	for i := range b64 {
		b64[i] &^= o64[i]
		num += bits.OnesCount64(b64[i])
	}
	setCardinality(b, num)
	return b
}
