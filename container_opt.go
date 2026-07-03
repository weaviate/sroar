package sroar

import (
	"math/bits"
	"slices"
)

var emptyArrayContainer []uint16

func init() {
	emptyArrayContainer = make([]uint16, minContainerSize)
	emptyArrayContainer[indexType] = typeArray
	emptyArrayContainer[indexSize] = uint16(len(emptyArrayContainer))
	setCardinality(emptyArrayContainer, 0)
}

// andNotDenseByHeaders reports, from cardinality headers alone, that (ac &^ bc)
// keeps at least andNotCompactThreshold values: bc can remove at most its own
// cardinality. Shared by the sizing pass and the materialization pass so both
// take the same dense-path decision. A corrupt header can only misroute to the
// dense path, which is sized maxContainerSize and preserves membership either
// way.
func andNotDenseByHeaders(ac, bc []uint16) bool {
	return getCardinality(ac)-getCardinality(bc) >= andNotCompactThreshold
}

// bitmapCardClamped counts the set bits of bitmap container c, returning bound
// as soon as the count reaches it.
func bitmapCardClamped(c []uint16, bound int) int {
	c64 := uint16To64SliceUnsafe(c[startIdx:])
	n := 0
	for i := range c64 {
		n += bits.OnesCount64(c64[i])
		if n >= bound {
			return bound
		}
	}
	return n
}

// andNotResultCard returns the cardinality of (ac &^ bc) without materializing
// the result and without allocating. bc == nil means no matching container.
// For bitmap sources the value is exact only below andNotCompactThreshold;
// results at least that large may be clamped to the threshold (their size in
// the arena does not depend on the exact count). Bitmap containers are counted
// by their bits, never their cardinality header, so a corrupt header cannot
// yield a negative or under-real count.
func andNotResultCard(ac, bc []uint16) int {
	if bc == nil {
		if ac[indexType] == typeBitmap {
			return bitmapCardClamped(ac, andNotCompactThreshold)
		}
		return getCardinality(ac)
	}
	at, bt := ac[indexType], bc[indexType]
	switch {
	case at == typeArray && bt == typeArray:
		av := ac[startIdx : int(startIdx)+getCardinality(ac)]
		bv := bc[startIdx : int(startIdx)+getCardinality(bc)]
		return len(av) - intersection2by2Cardinality(av, bv)
	case at == typeArray && bt == typeBitmap:
		n := 0
		for _, x := range ac[startIdx : int(startIdx)+getCardinality(ac)] {
			if !bitmap(bc).has(x) {
				n++
			}
		}
		return n
	case at == typeBitmap && bt == typeArray:
		if andNotDenseByHeaders(ac, bc) {
			return andNotCompactThreshold
		}
		bn := getCardinality(bc)
		n := bitmapCardClamped(ac, andNotCompactThreshold+bn)
		if n >= andNotCompactThreshold+bn {
			return andNotCompactThreshold // dense regardless of overlap
		}
		for _, x := range bc[startIdx : int(startIdx)+bn] {
			if bitmap(ac).has(x) {
				n--
			}
		}
		return n
	default: // bitmap, bitmap
		if andNotDenseByHeaders(ac, bc) {
			return andNotCompactThreshold
		}
		a64 := uint16To64SliceUnsafe(ac[startIdx:])
		b64 := uint16To64SliceUnsafe(bc[startIdx:])
		n := 0
		for i := range a64 {
			n += bits.OnesCount64(a64[i] &^ b64[i])
			if n >= andNotCompactThreshold {
				return andNotCompactThreshold // dense: exact count not needed
			}
		}
		return n
	}
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
	out := c
	if runMode&runInline == 0 {
		out = optBuf
		if out == nil {
			min := min(cnum, onum)
			out = make([]uint16, roundSize(startIdx+uint16(min)))
		}
	}
	setc := c.all()
	seto := other.all()
	num := intersection2by2(setc, seto, out[startIdx:])
	lastIdx := startIdx + uint16(num)

	if runMode&runInline == 0 {
		return bufAsArray(out, lastIdx)
	}
	setCardinality(c, num)
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

	if optBuf == nil {
		// Two-pointer scan: process each uint64 word of b against sorted
		// elements of other — no scratch buffer or allocation needed.
		// For each word, the uint64 mask is built on the fly from other's elements.
		// Bit of value x in dst64[x>>6]: bitmapMask reverses bit order within
		// each uint16 (bitmapMask[p]=1<<(15-p)), so the mapping is
		// uint64(bitmapMask[pos]) << (idx*16) where idx=(x>>4)&3, pos=x&0xF.
		// Benchmarks show this is faster than the previous 1KB batch approach
		// and avoids any allocation.
		dst64 := uint16To64SliceUnsafe(b[startIdx:])
		src := other.all()
		j := 0
		num := 0
		for i := range dst64 {
			wordEnd := (i + 1) * 64 // int: avoids uint16 overflow at i=1023
			if j >= len(src) || int(src[j]) >= wordEnd {
				dst64[i] = 0
				continue
			}
			if dst64[i] == 0 {
				for j < len(src) && int(src[j]) < wordEnd {
					j++
				}
				continue
			}
			var mask uint64
			for j < len(src) && int(src[j]) < wordEnd {
				x := src[j]
				idx := (x >> 4) & 3 // which uint16 within the uint64 (0-3)
				pos := x & 0xF      // bit within that uint16
				mask |= uint64(bitmapMask[pos]) << (idx * 16)
				j++
			}
			dst64[i] &= mask
			num += bits.OnesCount64(dst64[i])
		}
		setCardinality(b, num)
		return nil
	}

	clear(optBuf[startIdx:])
	for _, x := range other.all() {
		idx := x >> 4
		pos := x & 0xF
		optBuf[startIdx+idx] |= bitmapMask[pos]
	}

	dst64 := uint16To64SliceUnsafe(b[startIdx:])
	src64 := uint16To64SliceUnsafe(optBuf[startIdx:])
	var num int
	if bnum < maxCardinality/2 {
		// Sparse path: skip zero words — same threshold as andBitmapAlt.
		for i := range dst64 {
			if dst64[i] != 0 {
				dst64[i] &= src64[i]
				num += bits.OnesCount64(dst64[i])
			}
		}
	} else {
		for i := range dst64 {
			dst64[i] &= src64[i]
			num += bits.OnesCount64(dst64[i])
		}
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

	// Non-inline: copy b into out first, then AND out with other in place.
	//
	// The seemingly redundant copy is intentional and faster than a direct
	// 3-way AND (dst = b & other). The copy pre-warms out in L1 cache so
	// the subsequent AND loop operates as a 2-stream read-modify-write
	// (out + other), which is more efficient than a 3-stream operation
	// (read b, read other, write out). Benchmarks confirmed the copy+AND
	// approach is ~25% faster than the 3-way AND on ARM64 (Apple M4 Pro).
	out := b
	if runMode&runInline == 0 {
		out = copyBitmap(b, optBuf)
	}

	dst64 := uint16To64SliceUnsafe(out[startIdx:])
	src64 := uint16To64SliceUnsafe(other[startIdx:])
	var num int
	if bnum < maxCardinality/2 {
		// Sparse path: skip zero words to avoid AND+POPCNT on empty words.
		// Faster below ~50% fill; above that the branch overhead exceeds the benefit.
		// Only bnum (density of dst) matters — dst64 words are from b/out.
		for i := range dst64 {
			if dst64[i] != 0 {
				dst64[i] &= src64[i]
				num += bits.OnesCount64(dst64[i])
			}
		}
	} else {
		for i := range dst64 {
			dst64[i] &= src64[i]
			num += bits.OnesCount64(dst64[i])
		}
	}
	setCardinality(out, num)

	if runMode&runInline == 0 {
		return out
	}
	return nil
}

func containerIntersects(ac, bc []uint16) bool {
	at := ac[indexType]
	bt := bc[indexType]
	if at == typeArray && bt == typeArray {
		return array(ac).intersectsArray(array(bc))
	}
	if at == typeArray && bt == typeBitmap {
		return array(ac).intersectsBitmap(bitmap(bc))
	}
	if at == typeBitmap && bt == typeArray {
		return array(bc).intersectsBitmap(bitmap(ac))
	}
	if at == typeBitmap && bt == typeBitmap {
		return bitmap(ac).intersectsBitmap(bitmap(bc))
	}
	panic("containerIntersects: We should not reach here")
}

func (c array) intersectsArray(other array) bool {
	cs := c.all()
	os := other.all()
	ci, oi := 0, 0
	for ci < len(cs) && oi < len(os) {
		if cs[ci] == os[oi] {
			return true
		} else if cs[ci] < os[oi] {
			ci++
		} else {
			oi++
		}
	}
	return false
}

func (c array) intersectsBitmap(other bitmap) bool {
	return slices.ContainsFunc(c.all(), other.has)
}

func (b bitmap) intersectsBitmap(other bitmap) bool {
	bs := uint16To64SliceUnsafe(b[startIdx:])
	os := uint16To64SliceUnsafe(other[startIdx:])
	for i := range bs {
		if bs[i]&os[i] != 0 {
			return true
		}
	}
	return false
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
	out := c
	if runMode&runInline == 0 {
		out = optBuf
		if out == nil {
			out = make([]uint16, roundSize(startIdx+uint16(cnum)))
		}
	}
	setc := c.all()
	seto := other.all()
	num := difference(setc, seto, out[startIdx:])
	lastIdx := startIdx + uint16(num)

	if runMode&runInline == 0 {
		return bufAsArray(out, lastIdx)
	}
	setCardinality(c, num)
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
		clear(out[startIdx:])
		out[indexType] = typeBitmap
		out[indexSize] = maxContainerSize

		smaller, larger := other, c
		if onum > cnum {
			smaller, larger = c, other
		}

		// larger is ORed into an empty bitmap so every element is new —
		// no duplicate check needed and cardinality equals larger's size.
		num := max(cnum, onum)
		for _, x := range larger.all() {
			idx := x >> 4
			pos := x & 0xF
			out[startIdx+idx] |= bitmapMask[pos]
		}
		// smaller may overlap with larger so check each bit before counting.
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

	// merge: copy other into out then set c's bits per-element, counting
	// only new bits. Faster than clear+convert+OR+POPCNT at all cardinalities
	// because it avoids the 256-word OR+POPCNT sweep (~130ns fixed overhead).
	out := buf
	copy(out, other)
	addnum := 0
	for _, x := range c.all() {
		idx := x >> 4
		pos := x & 0xF
		if has := out[startIdx+idx]&bitmapMask[pos] > 0; !has {
			out[startIdx+idx] |= bitmapMask[pos]
			addnum++
		}
	}
	setCardinality(out, onum+addnum)

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

	var addnum int
	if bnum == 0 {
		// Bitmap is empty — every element is new, skip the !has check.
		// A full POPCNT sweep to recompute cardinality would cost ~500ns
		// fixed overhead; since all bits are new, addnum == onum.
		for _, x := range other.all() {
			out[startIdx+x>>4] |= bitmapMask[x&0xF]
		}
		addnum = onum
	} else {
		// Per-element check is faster than OR-all + POPCNT sweep for all
		// valid array cardinalities (< 2456 before bitmap conversion).
		for _, x := range other.all() {
			idx := x >> 4
			pos := x & 0xF
			if has := out[startIdx+idx]&bitmapMask[pos] > 0; !has {
				out[startIdx+idx] |= bitmapMask[pos]
				addnum++
			}
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

	// Non-inline: copy b into out first, then OR other into out in place.
	// The copy is intentional — it pre-warms out in L1 cache so the OR loop
	// operates as a 2-stream read-modify-write (out + other), which is faster
	// than a 3-stream operation (read b, read other, write out).
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
