/*
 * Copyright 2021 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sroar

import (
	"fmt"
	"math"
	"math/bits"
	"strings"
)

// container uses extra 4 []uint16 in the front as header.
// container[0] is used for storing the size of the container, expressed in Uint16.
// The container size cannot exceed the vicinity of 8KB. At 8KB, we switch from packed arrays to
// bitmaps. We can fit the entire uint16 worth of bitmaps in 8KB (2^16 / 8 = 8
// KB).

const (
	typeArray  uint16 = 0x00
	typeBitmap uint16 = 0x01

	// Container header.
	indexSize        int = 0
	indexType        int = 1
	indexCardinality int = 2
	// Index 2 and 3 is used for cardinality. We need 2 uint16s to store cardinality because
	// 2^16 will not fit in uint16.
	startIdx uint16 = 4

	minContainerSize = 64 // In Uint16.
	// Bitmap container can contain 2^16 integers. Each integer would use one bit to represent.
	// Given that our data is represented in []uint16s, that'd mean the size of container to store
	// it would be divided by 16.
	// 4 for header and 4096 for storing bitmap container. In Uint16.
	maxContainerSize = 4 + (1<<16)/16 // 4100
	// An array container never grows past half a bitmap container's payload.
	// That is the size expandContainer converts at, so the sizes the
	// exact-size constructors emit and the sizes the growth path produces
	// stop at the same boundary. In Uint16.
	maxArrayContainerSize = (1 << 16) / 16 / 2 // 2048
)

func incrCardinality(data []uint16) {
	cur := getCardinality(data)
	if cur+1 > math.MaxUint16 {
		data[indexCardinality+1] = 1
	} else {
		data[indexCardinality]++
	}
}

var invalidCardinality int = math.MaxUint16 + 10
var maxCardinality int = math.MaxUint16 + 1

func getCardinality(data []uint16) int {
	// This sum has to be done using two ints to avoid overflow.
	return int(data[indexCardinality]) + int(data[indexCardinality+1])
}

func isEmpty(data []uint16) bool {
	return data[indexCardinality]|data[indexCardinality+1] == 0
}

func setCardinality(data []uint16, c int) {
	if c > math.MaxUint16 {
		data[indexCardinality] = math.MaxUint16
		data[indexCardinality+1] = uint16(c - math.MaxUint16)
	} else {
		data[indexCardinality] = uint16(c)
		data[indexCardinality+1] = 0
	}
}

// setArrayHeader stamps c's header as an array container of the given
// cardinality, sized to c's full length.
func setArrayHeader(c []uint16, card int) {
	c[indexSize] = uint16(len(c))
	c[indexType] = typeArray
	setCardinality(c, card)
}

// zeroOutContainer empties c. A zero cardinality header implies the payload
// is already zero — every writer that zeroes the header also zeroes the
// payload — so the 8KB clear is skipped then (And-style passes re-zero the
// same emptied containers repeatedly).
func zeroOutContainer(c []uint16) {
	if isEmpty(c) {
		return
	}
	c[indexCardinality] = 0
	c[indexCardinality+1] = 0
	if c[indexType] == typeBitmap {
		clear(c[startIdx:c[indexSize]])
	}
}

func removeRangeContainer(c []uint16, lo, hi uint16) {
	switch c[indexType] {
	case typeArray:
		array(c).removeRange(lo, hi)
	case typeBitmap:
		bitmap(c).removeRange(lo, hi)
	}
}

func calculateAndSetCardinality(data []uint16) {
	if data[indexType] != typeBitmap {
		panic("Non-bitmap containers should always have cardinality set correctly")
	}
	b := bitmap(data)
	card := b.cardinality()
	setCardinality(b, card)
}

type array []uint16

// find returns the index of the first element >= x.
// The index is based on data portion of the container, ignoring startIdx.
// If the element > than all elements present, then N is returned where N = cardinality of the
// container.
func (c array) find(x uint16) int {
	N := getCardinality(c)
	si := int(startIdx)
	lo, hi := si, si+N-1
	for lo+8 <= hi {
		mid := lo + (hi-lo)/2
		// fmt.Printf("lo: %d mid: %d hi: %d. ki: %#x k: %#x\n", lo, mid, hi, c[mid], x)

		if c[mid] < x {
			lo = mid + 1
		} else if c[mid] > x {
			// We should keep it equal, and not -1, because we'll take the first greater entry.
			hi = mid
		} else {
			// fmt.Printf("returning mid: %d\n", mid)
			return mid - si
		}
	}
	for ; lo <= hi; lo++ {
		// fmt.Printf("itr. lo: %d hi: %d. ki: %#x k: %#x\n", lo, hi, c[lo], x)
		if c[lo] >= x {
			return lo - si
		}
	}
	return N
}

func (c array) rank(x uint16) int {
	N := getCardinality(c)
	idx := c.find(x)
	if idx == N {
		return -1
	}
	return idx
}

func (c array) has(x uint16) bool {
	N := getCardinality(c)
	idx := c.find(x)
	if idx == N {
		return false
	}
	return c[int(startIdx)+idx] == x
}

func (c array) add(x uint16) bool {
	idx := c.find(x)
	N := getCardinality(c)
	offset := int(startIdx) + idx

	if int(idx) < N {
		if c[offset] == x {
			return false
		}
		// The entry at offset is the first entry, which is greater than x. Move it to the right.
		copy(c[offset+1:], c[offset:])
	}
	c[offset] = x
	incrCardinality(c)
	return true
}

func (c array) remove(x uint16) bool {
	idx := c.find(x)
	N := getCardinality(c)
	offset := int(startIdx) + idx

	if int(idx) < N {
		if c[offset] != x {
			return false
		}
		copy(c[offset:], c[offset+1:])
		setCardinality(c, N-1)
		return true
	}
	return false
}

func (c array) removeRange(lo, hi uint16) {
	if hi < lo {
		panic(fmt.Sprintf("args must satisfy lo <= hi, got lo: %d, hi: %d\n", lo, hi))
	}
	loIdx := c.find(lo)
	hiIdx := c.find(hi)

	st := int(startIdx)
	loVal := c[st+loIdx]
	N := getCardinality(c)

	// remove range doesn't intersect with any element in the array.
	if hi < loVal || loIdx == N {
		return
	}
	if hiIdx == N {
		if loIdx > 0 {
			c = c[:int(startIdx)+loIdx-1]
		} else {
			c = c[:int(startIdx)]
		}
		setCardinality(c, loIdx)
		return
	}
	if c[st+hiIdx] == hi {
		hiIdx++
	}
	copy(c[st+loIdx:], c[st+hiIdx:])
	setCardinality(c, N-hiIdx+loIdx)
}

func (c array) zeroOut() {
	setCardinality(c, 0)
}

// TODO: Figure out how memory allocation would work in these situations. Perhaps use allocator here?
func (c array) andArray(other array) []uint16 {
	min := min(getCardinality(c), getCardinality(other))

	setc := c.all()
	seto := other.all()

	out := make([]uint16, int(startIdx)+min+1)
	num := uint16(intersection2by2(setc, seto, out[startIdx:]))

	// Truncate out to how many values were found.
	out = out[:startIdx+num+1]
	setArrayHeader(out, int(num))
	return out
}

// TODO: We can do this operation in-place on the src array.
func (c array) andNotArray(other array, buf []uint16) []uint16 {
	max := getCardinality(c)
	out := make([]uint16, int(startIdx)+max+1)

	andRes := array(c.andArray(other)).all()
	srcVals := array(c).all()
	num := uint16(difference(srcVals, andRes, out[startIdx:]))

	// Truncate out to how many values were found.
	out = out[:startIdx+num+1]
	setArrayHeader(out, int(num))
	return out
}

func (c array) orArray(other array, buf []uint16, runMode int) []uint16 {
	// We ignore runInline for this call.

	max := getCardinality(c) + getCardinality(other)
	if max > 4096 {
		// Use bitmap container.
		out := bitmap(c.toBitmapContainer(buf))
		// For now, just keep it as a bitmap. No need to change if the
		// cardinality is smaller than 4096.
		out.orArray(other, nil, runMode|runInline)
		// Return out because out is pointing to buf. This would allow the
		// receiver to copy out.
		return out
	}

	// The output would be of typeArray.
	out := buf[:int(startIdx)+max]
	num := union2by2(c.all(), other.all(), out[startIdx:])
	setArrayHeader(out, num)
	return out
}

var tmp = make([]uint16, 8192)

func (c array) andBitmap(other bitmap) []uint16 {
	out := make([]uint16, int(startIdx)+getCardinality(c)+2) // some extra space.

	pos := startIdx
	for _, x := range c.all() {
		out[pos] = x
		pos += other.bitValue(x)
	}

	// Ensure we have at least one empty slot at the end.
	res := out[:pos+1]
	setArrayHeader(res, int(pos-startIdx))
	return res
}

// TODO: Write an optmized version of this function.
func (c array) andNotBitmap(other bitmap, buf []uint16) []uint16 {
	assert(len(buf) == maxContainerSize)
	res := array(buf)
	clear(res)
	res[indexSize] = 4
	for _, e := range c.all() {
		if !other.has(e) {
			if res.add(e) {
				res[indexSize]++
			}
		}
	}
	return res
}

func (c array) isFull() bool {
	N := getCardinality(c)
	return int(startIdx)+N >= len(c)
}

func (c array) all() []uint16 {
	N := getCardinality(c)
	return c[startIdx : int(startIdx)+N]
}

func (c array) minimum() uint16 {
	N := getCardinality(c)
	if N == 0 {
		return 0
	}
	return c[startIdx]
}

func (c array) maximum() uint16 {
	N := getCardinality(c)
	if N == 0 {
		return 0
	}
	return c[int(startIdx)+N-1]
}

func (c array) toBitmapContainer(buf []uint16) []uint16 {
	if len(buf) == 0 {
		buf = make([]uint16, maxContainerSize)
	} else {
		assert(len(buf) == maxContainerSize)
		clear(buf[startIdx:])
	}

	b := bitmap(buf)
	b[indexSize] = maxContainerSize
	b[indexType] = typeBitmap
	setCardinality(b, getCardinality(c))

	data := b[startIdx:]
	for _, x := range c.all() {
		setBit(data, x)
	}
	return b
}

func (c array) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Size: %d\n", c[0]))
	for i, val := range c[startIdx:] {
		b.WriteString(fmt.Sprintf("%d: %d\n", i, val))
	}
	return b.String()
}

type bitmap []uint16

// bitmapMask returns the bit for position pos within a bitmap word. Bit
// order is reversed within each uint16 — bitmapMask(p) == 1<<(15-p) — so the
// smallest position holds the highest bit (see bitsIntoArray).
func bitmapMask(pos uint16) uint16 {
	return 0x8000 >> pos
}

// setBit, clearBit and hasBit set, clear and test value x's bit in a
// headerless run of bitmap words (a bitmap container's data past its header,
// or an Accumulator staging block).
func setBit(words []uint16, x uint16) {
	words[x>>4] |= bitmapMask(x & 0xF)
}

func clearBit(words []uint16, x uint16) {
	words[x>>4] &^= bitmapMask(x & 0xF)
}

func hasBit(words []uint16, x uint16) bool {
	return words[x>>4]&bitmapMask(x&0xF) > 0
}

func (b bitmap) add(x uint16) bool {
	idx := x >> 4
	mask := bitmapMask(x & 0xF)

	if b[startIdx+idx]&mask > 0 {
		return false
	}

	b[startIdx+idx] |= mask
	incrCardinality(b)
	return true
}

func (b bitmap) remove(x uint16) bool {
	idx := x >> 4
	pos := x & 0xF

	c := getCardinality(b)
	if has := b[startIdx+idx] & bitmapMask(pos); has > 0 {
		b[startIdx+idx] &^= bitmapMask(pos)
		setCardinality(b, c-1)
		return true
	}
	return false
}

func (b bitmap) removeRange(lo, hi uint16) {
	loIdx := lo >> 4
	loPos := lo & 0xF

	hiIdx := hi >> 4
	hiPos := hi & 0xF

	N := getCardinality(b)
	var removed int
	for i := loIdx + 1; i < hiIdx; i++ {
		removed += bits.OnesCount16(b[startIdx+i])
		b[startIdx+i] = 0
	}

	if loIdx == hiIdx {
		for p := loPos; p <= hiPos; p++ {
			if b[startIdx+loIdx]&bitmapMask(p) > 0 {
				removed++
			}
			b[startIdx+loIdx] &= ^bitmapMask(p)
		}
		setCardinality(b, N-removed)
		return
	}
	for p := loPos; p < 1<<4; p++ {
		if b[startIdx+loIdx]&bitmapMask(p) > 0 {
			removed++
		}
		b[startIdx+loIdx] &= ^bitmapMask(p)
	}
	for p := uint16(0); p <= hiPos; p++ {
		if b[startIdx+hiIdx]&bitmapMask(p) > 0 {
			removed++
		}
		b[startIdx+hiIdx] &= ^bitmapMask(p)
	}
	setCardinality(b, N-removed)
}

func (b bitmap) has(x uint16) bool {
	return hasBit(b[startIdx:], x)
}

func (b bitmap) rank(x uint16) int {
	idx := x >> 4
	pos := x & 0xF
	if !hasBit(b[startIdx:], x) {
		return -1
	}

	var rank int
	for i := 0; i < int(idx); i++ {
		rank += bits.OnesCount16(b[int(startIdx)+i])
	}
	for p := uint16(0); p <= pos; p++ {
		if b[startIdx+idx]&bitmapMask(p) > 0 {
			rank++
		}
	}
	return rank - 1
}

// TODO: This can perhaps be using SIMD instructions.
func (b bitmap) andBitmap(other bitmap) []uint16 {
	out := make([]uint16, maxContainerSize)
	out[indexSize] = maxContainerSize
	out[indexType] = typeBitmap
	var num int
	for i := int(startIdx); i < len(b); i++ {
		out[i] = b[i] & other[i]
		num += bits.OnesCount16(out[i])
	}
	setCardinality(out, num)
	return out
}

func (b bitmap) orBitmap(other bitmap, buf []uint16, runMode int) []uint16 {
	if runMode&runInline > 0 {
		buf = b
	} else {
		copy(buf, b) // Copy over first.
	}
	buf[indexSize] = maxContainerSize
	buf[indexType] = typeBitmap

	if num := getCardinality(b); num == maxCardinality {
		// do nothing. bitmap is already full.

	} else if runMode&runLazy > 0 || num == invalidCardinality {
		data := buf[startIdx:]
		for i, v := range other[startIdx:] {
			data[i] |= v
		}
		setCardinality(buf, invalidCardinality)

	} else {
		var num int
		data := buf[startIdx:]
		for i, v := range other[startIdx:] {
			data[i] |= v
			// We are going to iterate over the entire container. So, we can
			// just recount the cardinality, starting from num=0.
			num += bits.OnesCount16(data[i])
		}
		setCardinality(buf, num)
	}
	if runMode&runInline > 0 {
		return nil
	}
	return buf
}

func (b bitmap) andNotBitmap(other bitmap) []uint16 {
	var num int
	data := b[startIdx:]
	for i, v := range other[startIdx:] {
		data[i] = data[i] ^ (data[i] & v)
		num += bits.OnesCount16(data[i])
	}
	setCardinality(b, num)
	return b
}

func (b bitmap) andNotArray(other array) []uint16 {
	for _, e := range other.all() {
		b.remove(e)
	}
	return b
}

func (b bitmap) orArray(other array, buf []uint16, runMode int) []uint16 {
	if runMode&runInline > 0 {
		buf = b
	} else {
		copy(buf, b)
	}

	if num := getCardinality(b); num == maxCardinality {
		// do nothing. This bitmap is already full.

	} else if runMode&runLazy > 0 || num == invalidCardinality {
		// Avoid calculating the cardinality to speed up operations.
		data := buf[startIdx:]
		for _, x := range other.all() {
			setBit(data, x)
		}
		setCardinality(buf, invalidCardinality)

	} else {
		// Branchless new-bit counting via popcount diff: kept over
		// hasBit/setBit to stay insensitive to the overlap distribution.
		num := getCardinality(buf)
		for _, x := range other.all() {
			idx := x / 16
			pos := x % 16

			val := &buf[4+idx]
			before := bits.OnesCount16(*val)
			*val |= bitmapMask(pos)
			after := bits.OnesCount16(*val)
			num += after - before
		}
		setCardinality(buf, num)
	}

	if runMode&runInline > 0 {
		return nil
	}
	return buf
}

func (b bitmap) all() []uint16 {
	res := make([]uint16, getCardinality(b))
	return res[:b.intoArray(res)]
}

// intoArray writes b's set values into out in ascending order and returns the
// number written. Stops when out is full, so a cardinality header that
// understates the popcount degrades deterministically rather than overrunning.
func (b bitmap) intoArray(out []uint16) int {
	return bitsIntoArray(b[startIdx:], out, 0)
}

// bitsIntoArray writes the set values of a headerless run of bitmap words (a
// bitmap container's data past its header, or an Accumulator staging block)
// into out in ascending order and returns the number written. baseWord is
// words' offset within its container, so callers can pass a clamped window
// and still extract the right values; pass 0 for a full run. Stops when out
// is full, so an out sized below the words' popcount degrades
// deterministically rather than overrunning.
func bitsIntoArray(words []uint16, out []uint16, baseWord uint16) int {
	idx := 0
	for w, word := range words {
		if word == 0 {
			continue
		}
		base := (baseWord + uint16(w)) << 4
		// bitmapMask(pos) is 1<<(15-pos), so the smallest pos is the highest
		// bit: LeadingZeros16 yields values in ascending order.
		for word != 0 {
			if idx == len(out) {
				return idx
			}
			pos := uint16(bits.LeadingZeros16(word))
			out[idx] = base | pos
			idx++
			word &^= 1 << (15 - pos)
		}
	}
	return idx
}

// andNotBitmapIntoArray writes the ascending survivors of (b &^ other) into out
// and returns the count, subtracting word by word with no intermediate bitmap.
func (b bitmap) andNotBitmapIntoArray(other bitmap, out []uint16) int {
	odata := other[startIdx:]
	idx := 0
	for w, word := range b[startIdx:] {
		if word &^= odata[w]; word == 0 {
			continue
		}
		base := uint16(w) << 4
		for word != 0 {
			if idx == len(out) {
				return idx
			}
			pos := uint16(bits.LeadingZeros16(word))
			out[idx] = base | pos
			idx++
			word &^= 1 << (15 - pos)
		}
	}
	return idx
}

// andNotArrayIntoArray writes the ascending survivors of (b &^ other) into out
// and returns the count, enumerating b's bits while a two-pointer walks other's
// sorted values to skip the removed ones.
func (b bitmap) andNotArrayIntoArray(other array, out []uint16) int {
	ovals := other.all()
	idx, j := 0, 0
	for w, word := range b[startIdx:] {
		if word == 0 {
			continue
		}
		base := uint16(w) << 4
		for word != 0 {
			if idx == len(out) {
				return idx
			}
			pos := uint16(bits.LeadingZeros16(word))
			word &^= 1 << (15 - pos)
			val := base | pos
			for j < len(ovals) && ovals[j] < val {
				j++
			}
			if j < len(ovals) && ovals[j] == val {
				continue // removed by the array subtrahend
			}
			out[idx] = val
			idx++
		}
	}
	return idx
}

// TODO: It can be optimized.
func (b bitmap) selectAt(idx int) uint16 {
	data := b[startIdx:]
	n := uint16(len(data))
	for i := uint16(0); i < n; i++ {
		x := data[i]
		c := bits.OnesCount16(x)
		if idx < c {
			for pos := uint16(0); pos < 16; pos++ {
				if idx == 0 && x&bitmapMask(pos) > 0 {
					return i*16 + pos
				}
				if x&bitmapMask(pos) > 0 {
					idx--
				}
			}

		}
		idx -= c
	}
	panic("should not reach here")
}

// bitValue returns a 0 or a 1 depending upon whether x is present in the bitmap, where 1 means
// present and 0 means absent.
func (b bitmap) bitValue(x uint16) uint16 {
	idx := x >> 4
	return (b[4+idx] >> (15 - (x & 0xF))) & 1
}

func (b bitmap) isFull() bool {
	return false
}

func (b bitmap) minimum() uint16 {
	if N := getCardinality(b); N == 0 {
		return 0
	}

	b64 := uint16To64SliceUnsafe(b[startIdx:])
	for i := 0; i < len(b64); i++ {
		if b64[i] != 0 {
			for j := 0; j < 4; j++ {
				idx := i*4 + j
				if lz := bits.LeadingZeros16(b[idx+int(startIdx)]); lz != 16 {
					return uint16(16*idx + lz)
				}
			}
			break
		}
	}
	panic("We shouldn't reach here")
}

func (b bitmap) maximum() uint16 {
	if N := getCardinality(b); N == 0 {
		return 0
	}

	b64 := uint16To64SliceUnsafe(b[startIdx:])
	for i := len(b64) - 1; i >= 0; i-- {
		if b64[i] != 0 {
			for j := 3; j >= 0; j-- {
				idx := i*4 + j
				if tz := bits.TrailingZeros16(b[idx+int(startIdx)]); tz != 16 {
					return uint16(16*idx + 15 - tz)
				}
			}
			break
		}
	}
	panic("We shouldn't reach here")
}

func (b bitmap) cardinality() int {
	var num int
	for _, x := range b[startIdx:] {
		num += bits.OnesCount16(x)
	}
	return num
}

func (b bitmap) zeroOut() {
	setCardinality(b, 0)
	clear(b[startIdx:b[indexSize]])
}

var (
	runInline = 0x01
	runLazy   = 0x02
)

func containerOr(ac, bc, buf []uint16, runMode int) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		// We can't always inline this function. If the right container has
		// enough entries, trying to do a union with the left container inplace
		// could end up overwriting the left container entries. So, we use a
		// buffer to hold all output, and then copy it over to left.
		//
		// TODO: If right doesn't have a lot of entries, we could just iterate
		// over left and merge the entries from right inplace. Would be faster
		// than copying over all entries into buffer. Worth trying that approach.
		return left.orArray(right, buf, runMode)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		// Don't run inline for this call.
		return right.orArray(left, buf, runMode&^runInline)
	}

	// These two following cases can be fully inlined.
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.orArray(right, buf, runMode)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.orBitmap(right, buf, runMode)
	}
	panic("containerOr: We should not reach here")
}

func containerAnd(ac, bc []uint16) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andArray(right)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andBitmap(right)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		out := right.andBitmap(left)
		return out
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andBitmap(right)
	}
	panic("containerAnd: We should not reach here")
}

// TODO: Optimize this function.
func containerAndNot(ac, bc, buf []uint16) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andNotArray(right, buf)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andNotBitmap(right, buf)
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
		return left.andNotBitmap(right)
	}
	panic("containerAndNot: We should not reach here")
}

// nextGeq returns the smallest set value >= y in bitmap container b, and
// whether one exists.
func (b bitmap) nextGeq(y uint16) (uint16, bool) {
	data := b[startIdx:]
	w := int(y >> 4)
	// bitmapMask(pos) is 1<<(15-pos), so positions >= y&15 occupy the low bits
	// of the word: keep them, drop the earlier (higher) bits.
	word := data[w] & (0xFFFF >> (y & 0xF))
	for {
		if word != 0 {
			pos := uint16(bits.LeadingZeros16(word))
			return uint16(w)<<4 | pos, true
		}
		w++
		if w >= len(data) {
			return 0, false
		}
		word = data[w]
	}
}
