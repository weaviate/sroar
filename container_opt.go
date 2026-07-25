package sroar

import (
	"math"
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

// andNotResultCard returns the cardinality of (ac &^ bc) without materializing
// or allocating, clamped at andNotCompactThreshold (above it the exact count is
// unused, as the source stays a maxContainerSize bitmap). A nil or empty bc
// leaves ac unchanged.
//
// Bitmap sources are counted from the cardinality header, not by recounting
// bits: bc removes at most bcCard values, so acCard-bcCard >= andNotCompactThreshold
// proves a dense result without any counting.
func andNotResultCard(ac, bc []uint16) int {
	// Both counts drive arithmetic and the result's storage form, so lazy
	// headers have to be settled rather than clamped.
	acCard := containerCardinality(ac)
	bcCard := 0
	if bc != nil {
		bcCard = containerCardinality(bc)
	}
	if bcCard == 0 {
		// nothing to subtract: result is ac unchanged
		if ac[indexType] == typeBitmap {
			// clamp: a full bitmap's cardinality overflows the caller's uint16,
			// and the exact value above the threshold is unused
			return min(acCard, andNotCompactThreshold)
		}
		// an array result stays an array sized to this exact count, so no clamp
		return acCard
	}
	at, bt := ac[indexType], bc[indexType]
	switch {
	case at == typeArray && bt == typeArray:
		return acCard - intersection2by2Cardinality(array(ac).all(), array(bc).all())
	case at == typeArray && bt == typeBitmap:
		n := acCard
		for _, x := range array(ac).all() {
			if bitmap(bc).has(x) {
				n--
			}
		}
		return n
	case at == typeBitmap && bt == typeArray:
		if acCard-bcCard >= andNotCompactThreshold {
			return andNotCompactThreshold // dense by headers alone
		}
		// Start from acCard and drop each array element present in ac. n only
		// falls, so n-remaining is its floor; once that reaches the threshold
		// the result is dense no matter the rest.
		n := acCard
		remaining := bcCard
		for _, x := range array(bc).all() {
			remaining--
			if bitmap(ac).has(x) {
				n--
			}
			if n-remaining >= andNotCompactThreshold {
				return andNotCompactThreshold
			}
		}
		// the final iteration's floor check was n >= threshold, so here n < threshold
		return n
	case at == typeBitmap && bt == typeBitmap:
		if acCard-bcCard >= andNotCompactThreshold {
			return andNotCompactThreshold // dense by headers alone
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
	default:
		panic("andNotResultCard: We should not reach here")
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

	if runMode&runLazy > 0 {
		var orAcc uint64
		andAcc := uint64(math.MaxUint64)
		for i := range dst64 {
			w := dst64[i] & src64[i]
			dst64[i] = w
			orAcc |= w
			andAcc &= w
		}
		setCardinality(out, lazyCardinality(orAcc, andAcc))

		if runMode&runInline == 0 {
			return out
		}
		return nil
	}

	var num int
	// A lazy bnum is not below the threshold, so it lands on the dense loop.
	// That is the right default: the sparse loop only pays when whole words are
	// zero, which the density it can no longer read would have to prove.
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

// containerAndNotAlt computes ac &^ bc. A nil bc means nothing to subtract.
//
// Inline (runMode&runInline): subtracts bc from ac in place.
//
// Non-inline: writes the result into dst, whose length selects the output form —
// a full bitmap when len(dst) == maxContainerSize, otherwise a compact array.
// The result is produced straight into dst with no workspace: an array source
// subtracts into the slot; a dense bitmap is copied in and subtracted in place;
// a sparse bitmap has its survivors enumerated into the slot. A compact array's
// spare tail is left uninitialized — array reads never look past the cardinality.
func containerAndNotAlt(ac, bc, dst []uint16, runMode int) []uint16 {
	if runMode&runInline == 0 {
		out := dst[startIdx:]
		var w int
		switch ac[indexType] {
		case typeArray:
			a := array(ac)
			switch {
			case bc == nil:
				w = copy(out, a.all())
			case bc[indexType] == typeArray:
				w = a.andNotArrayIntoArray(array(bc), out)
			case bc[indexType] == typeBitmap:
				w = a.andNotBitmapIntoArray(bitmap(bc), out)
			default:
				panic("containerAndNot: unknown bc container type")
			}
		case typeBitmap:
			if len(dst) == maxContainerSize {
				// dense result stays a bitmap: subtract in place inside the slot
				copy(dst, ac)
				containerAndNotAlt(dst, bc, nil, runInline)
				return dst
			}
			// sparse result: enumerate the survivors into the slot as an array
			b := bitmap(ac)
			switch {
			case bc == nil:
				w = b.intoArray(out)
			case bc[indexType] == typeArray:
				w = b.andNotArrayIntoArray(array(bc), out)
			case bc[indexType] == typeBitmap:
				w = b.andNotBitmapIntoArray(bitmap(bc), out)
			default:
				panic("containerAndNot: unknown bc container type")
			}
		default:
			panic("containerAndNot: unknown ac container type")
		}
		setArrayHeader(dst, w)
		return dst
	}

	if bc == nil {
		return nil // subtracting nothing leaves ac unchanged
	}
	at := ac[indexType]
	bt := bc[indexType]
	switch {
	case at == typeArray && bt == typeArray:
		array(ac).andNotArrayAlt(array(bc))
	case at == typeArray && bt == typeBitmap:
		array(ac).andNotBitmapAlt(bitmap(bc))
	case at == typeBitmap && bt == typeArray:
		bitmap(ac).andNotArrayAlt(array(bc))
	case at == typeBitmap && bt == typeBitmap:
		bitmap(ac).andNotBitmapAlt(bitmap(bc))
	default:
		panic("containerAndNot: We should not reach here")
	}
	return nil
}

// The four andNot*Alt methods subtract other from the receiver in place. An
// empty receiver or an empty other leaves the receiver untouched.

func (c array) andNotArrayAlt(other array) {
	if getCardinality(c) == 0 || getCardinality(other) == 0 {
		return
	}
	setCardinality(c, c.andNotArrayIntoArray(other, c[startIdx:]))
}

func (c array) andNotBitmapAlt(other bitmap) {
	if getCardinality(c) == 0 || getCardinality(other) == 0 {
		return
	}
	setCardinality(c, c.andNotBitmapIntoArray(other, c[startIdx:]))
}

// andNotArrayIntoArray writes the ascending values of (c &^ other) into out,
// returning the count. Array minus array is exactly a set difference.
func (c array) andNotArrayIntoArray(other array, out []uint16) int {
	return difference(c.all(), other.all(), out)
}

// andNotBitmapIntoArray writes the ascending values of (c &^ other) into out
// and returns the count, keeping the values not present in other.
func (c array) andNotBitmapIntoArray(other bitmap, out []uint16) int {
	w := 0
	for _, x := range c.all() {
		if !other.has(x) {
			out[w] = x
			w++
		}
	}
	return w
}

func (b bitmap) andNotArrayAlt(other array) {
	if getCardinality(b) == 0 || getCardinality(other) == 0 {
		return
	}
	// The result is counted as bnum-delnum, so a lazy bnum has to be settled.
	bnum := containerCardinality(b)
	delnum := 0
	for _, x := range other.all() {
		idx := x >> 4
		pos := x & 0xF
		if b[startIdx+idx]&bitmapMask[pos] > 0 {
			b[startIdx+idx] ^= bitmapMask[pos]
			delnum++
		}
	}
	setCardinality(b, bnum-delnum)
}

func (b bitmap) andNotBitmapAlt(other bitmap) {
	if getCardinality(b) == 0 || getCardinality(other) == 0 {
		return
	}
	dst64 := uint16To64SliceUnsafe(b[startIdx:])
	src64 := uint16To64SliceUnsafe(other[startIdx:])
	var num int
	for i := range dst64 {
		dst64[i] &^= src64[i]
		num += bits.OnesCount64(dst64[i])
	}
	setCardinality(b, num)
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
	// The result is counted as onum+addnum, so a lazy onum has to be settled.
	onum = containerCardinality(other)
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

	// The result is counted as bnum+addnum, so a lazy bnum has to be settled.
	// Array operands are rare on the merge paths that go lazy, so this recount
	// is not on the path the laziness is there to speed up.
	bnum = containerCardinality(b)

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

	if runMode&runLazy > 0 {
		var orAcc uint64
		andAcc := uint64(math.MaxUint64)
		for i := range dst64 {
			w := dst64[i] | src64[i]
			dst64[i] = w
			orAcc |= w
			andAcc &= w
		}
		setCardinality(out, lazyCardinality(orAcc, andAcc))

		if runMode&runInline == 0 {
			return out
		}
		return nil
	}

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
	setArrayHeader(out, cnum)
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
	setArrayHeader(out, int(lastIdx-startIdx))
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
