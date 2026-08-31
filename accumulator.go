package sroar

import (
	"fmt"
	"math/bits"
	"slices"
)

// Accumulator combines many bitmaps into one by depositing their elements
// into dense per-key staging containers instead of merging roaring
// structures pairwise. Merging N tiny bitmaps via Or/FastOr costs a
// structural container merge per source — for array containers that is an
// O(container size) memmove to insert even a single element, which makes
// N-way unions of near-singleton bitmaps quadratic in the source count. The
// accumulator makes every deposit O(1) by staging each touched 64K key
// range in one fixed-size dense block, then assembling the exact-size
// result once (Bitmap, BitmapToBuf or InitBitmapToBuf).
//
// AndNot removes elements the same way — bit-clears into the staged ranges,
// skipping ranges the accumulator never touched — so Or and AndNot can
// interleave freely (e.g. per source layer: subtract its deletions, then
// deposit its additions). The result reflects every call applied in order.
// OrAcc and AndNotAcc do the same with another accumulator's staged state
// as the source, so partial accumulators built separately can be merged.
//
// Total cost is O(total input elements + touched key ranges); staging
// memory is one 8KB block per touched key range — proportional to the
// spread of everything deposited, not to the number of sources; a range
// AndNot empties keeps its block until Reset. In the sparse
// extreme — every element in its own range — that degrades to ~8KB per
// element, staging orders of magnitude more memory than the result it
// builds; FastOr or sort+FromSortedList are the better tools there. Reset
// caps only what is retained between unions, never the peak during one.
// Within one source, keys ascend, so its new ranges append cheaply; across
// sources, each new range that arrives out of order pays an O(existing
// ranges) insertion shift, so unions spanning many ranges fed from
// unsorted sources approach O(ranges²) in that term.
//
// Usage:
//
//	acc := NewAccumulator()
//	for _, bm := range sources {
//		acc.Or(bm)
//	}
//	result := acc.Bitmap()
//
// Sources are only read, never retained, so callers may release or reuse a
// source's memory as soon as the depositing call returns. The zero
// Accumulator is ready to use; NewAccumulator is equivalent. An Accumulator
// is not safe for concurrent use: any parallelism happens inside a single
// call (see WithConc), never across calls.
type Accumulator struct {
	// keys holds the touched 64K-range keys in ascending order; blocks is
	// parallel to it. Each block is a headerless 4096-uint16 payload — the
	// 64K bits of its range in bitmap-container bit order (bitmapMask), but
	// without the container metadata, which only the final containers need.
	// No cardinality is maintained during Or; it is computed once (via
	// popcount) when the result is built.
	keys   []uint64
	blocks [][]uint16

	// free holds spare blocks carried across Reset (at most maxRetained),
	// kept dirty — insertBlockAt zeroes one when it is claimed. Unlike
	// blocks, entries are not bound to any key: the next union's ranges —
	// whatever they are — claim them before allocating anew.
	free [][]uint16

	// lastIdx caches the most recently hit index into keys/blocks: sources
	// frequently cluster in one range (and single-container sources under a
	// small doc-ID universe hit very few ranges), so checking it first
	// skips the binary search on runs of same-range deposits.
	lastIdx int

	// gen counts every non-nil Or/OrAcc/AndNot/AndNotAcc and Reset call.
	// Builds compare it across the get callback to catch a get that uses
	// the accumulator mid-build — the layout snapshot would no longer
	// match the staged bits.
	gen uint64

	// maxConc caps the goroutines a single deposit call (Or, OrAcc,
	// AndNot, AndNotAcc) may fan out to; concCap resolves it. Values <= 1
	// (including the zero Accumulator) stay serial.
	maxConc int

	// maxRetained caps the staging blocks Reset keeps as spares for reuse.
	// 0 (including the zero Accumulator) retains nothing; block reuse is
	// opt-in via WithRetainedBlocks.
	maxRetained int
}

// NewAccumulator returns an accumulator whose calls run entirely on the
// calling goroutine and whose Reset retains no staging blocks; chain
// WithConc and WithRetainedBlocks to enable in-call parallelism and
// cross-Reset block reuse.
func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

// WithConc sets the cap on goroutines a single deposit call (Or, OrAcc,
// AndNot, AndNotAcc) may split its work across, and returns the
// accumulator so it chains with construction: NewAccumulator().WithConc(8).
// Values <= 1 keep every call serial; there is no unbounded sentinel — pass
// a large value (e.g. math.MaxInt) to let fan-out track the available work.
// The cap engages only when there is enough work to pay for the fan-out (the
// Or variants gate on the source's container or range count, the AndNot
// variants on the smaller of the two key sets) and survives Reset — pool
// users may reconfigure a checked-out accumulator between calls.
func (acc *Accumulator) WithConc(maxConcurrency int) *Accumulator {
	acc.maxConc = maxConcurrency
	return acc
}

// concCap resolves the configured cap to a concrete goroutine limit of at
// least 1: the never-configured accumulator and any value <= 1 stay serial,
// a large value lets calcConcurrency size the fan-out to the work.
func (acc *Accumulator) concCap() int {
	return max(1, acc.maxConc)
}

// WithRetainedBlocks caps how many staging blocks Reset keeps as spares for
// the next union to reuse (each is one touched 64K range, ~8KB), and returns
// the accumulator so it chains with construction. The slices indexing those
// blocks are bounded by the same budget (see retainedSlotFactor), so a union
// within it carries its whole staging layout across Reset. Values <= 0 retain
// nothing — the default, so a pooled accumulator opts into block reuse
// explicitly; pass a large value to retain every block a union touched. The
// setting survives Reset, so pool users configure it once. Retention bounds
// carried memory only; the peak during a single union is whatever its spread
// demands.
func (acc *Accumulator) WithRetainedBlocks(blocks int) *Accumulator {
	acc.maxRetained = max(0, blocks)
	return acc
}

// stageBlock returns the staging block for the 64K range of key, creating it
// on first touch; created reports a fresh block. A created block is zeroed
// unless overwrite is set — which promises the caller will fully overwrite it
// (a dense copy), letting a reused spare be handed back dirty and skip the
// clear. overwrite is ignored when the block already exists (never a fresh
// spare). The single lookup keeps the deposit loops at one call per container.
func (acc *Accumulator) stageBlock(key uint64, overwrite bool) (b []uint16, created bool) {
	keys := acc.keys
	if acc.lastIdx < len(keys) && keys[acc.lastIdx] == key {
		return acc.blocks[acc.lastIdx], false
	}
	// A local binary search keeps the hot deposit path free of the
	// generic slices.BinarySearch call.
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := (lo + hi) >> 1
		if keys[mid] < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(keys) && keys[lo] == key {
		acc.lastIdx = lo
		return acc.blocks[lo], false
	}
	return acc.blocks[acc.insertBlockAt(lo, key, overwrite)], true
}

// insertBlockAt inserts a staging block for key at lo, the insertion point
// stageBlock computed, and returns lo. The block is zeroed unless overwrite
// promises the caller will fill it. Ranges track the union's spread, not its
// source count, so the shift that keeps keys sorted stays small.
func (acc *Accumulator) insertBlockAt(lo int, key uint64, overwrite bool) int {
	var b []uint16
	if n := len(acc.free) - 1; n >= 0 {
		// Nil the vacated slot so the backing array stops referencing a claimed
		// block. Spares are carried dirty, so the clear is paid here — and only
		// when the caller isn't about to overwrite the block anyway.
		b, acc.free[n], acc.free = acc.free[n], nil, acc.free[:n]
		if !overwrite {
			clear(b)
		}
	} else {
		b = make([]uint16, maxContainerSize-int(startIdx))
	}
	acc.keys = append(acc.keys, 0)
	acc.blocks = append(acc.blocks, nil)
	copy(acc.keys[lo+1:], acc.keys[lo:])
	copy(acc.blocks[lo+1:], acc.blocks[lo:])
	acc.keys[lo] = key
	acc.blocks[lo] = b
	acc.lastIdx = lo
	return lo
}

// searchFrom returns the smallest index in (from, to) at which acc.keys[i]
// >= k, or to if there is none: exponential expansion to bracket k, then
// binary search — O(log gap). from may be -1 to include index 0 in the
// search.
func (acc *Accumulator) searchFrom(from, to int, k uint64) int {
	keys := acc.keys
	lower := from + 1
	if lower >= to || keys[lower] >= k {
		return lower
	}
	span := 1
	for lower+span < to && keys[lower+span] < k {
		span *= 2
	}
	upper := lower + span
	if upper >= to {
		upper = to - 1
	}
	if keys[upper] < k {
		return to
	}
	// Binary search within [lower + span/2, upper].
	lower += span >> 1
	for lower+1 < upper {
		mid := (lower + upper) >> 1
		switch {
		case keys[mid] < k:
			lower = mid
		case keys[mid] > k:
			upper = mid
		default:
			return mid
		}
	}
	return upper
}

// Or deposits all elements of bm into the accumulator. bm is only read,
// never retained: it may be released or reused as soon as Or returns.
func (acc *Accumulator) Or(bm *Bitmap) {
	if bm == nil {
		return
	}
	acc.gen++
	n := bm.keys.numKeys()
	conc := calcConcurrency(n, minContainersPerRoutine, acc.concCap())
	if conc <= 1 {
		for i := 0; i < n; i++ {
			off := bm.keys.val(i)
			if getCardinality(bm.data[off:]) == 0 {
				continue
			}
			src := bm.getContainer(off)
			// A dense source into a fresh block is adopted by copy — cheaper
			// than OR-ing into zeros, and it makes the zeroing pointless.
			dst, created := acc.stageBlock(bm.keys.key(i), src[indexType] == typeBitmap)
			if created && src[indexType] == typeBitmap {
				copy(dst, src[startIdx:maxContainerSize])
				continue
			}
			switch src[indexType] {
			case typeArray:
				for _, lo := range array(src).all() {
					setBit(dst, lo)
				}
			case typeBitmap:
				d64 := uint16To64SliceUnsafe(dst)
				s64 := uint16To64SliceUnsafe(src[startIdx:maxContainerSize])
				for j, w := range s64 {
					d64[j] |= w
				}
			default:
				panic(fmt.Sprintf("Accumulator.Or: unknown container type %d", src[indexType]))
			}
		}
		return
	}
	// Create every missing block here, so workers only read the index; bm's
	// keys are unique, so they write disjoint blocks and need no locking. A
	// source of only emptied containers deposits nothing — skip the fan-out.
	isEmpty := true
	for i := 0; i < n; i++ {
		if getCardinality(bm.data[bm.keys.val(i):]) > 0 {
			acc.stageBlock(bm.keys.key(i), false) // orRange ORs, so it must be zeroed
			isEmpty = false
		}
	}
	if isEmpty {
		return
	}
	concurrentlyInRanges(n, conc, func(from, to, _ int) {
		acc.orRange(bm, from, to)
	})
}

// OrAcc deposits everything staged in other into the accumulator — the
// merge step for partial accumulators built separately. The receiver is
// the destination; other is only read, never retained, and must stay
// unmodified for the duration of the call. Its blocks are copied or
// OR-merged in, both accumulators stay independently usable, and OrAcc on
// itself is a no-op. Ranges other has staged but emptied (via AndNot) are
// staged here too, contributing nothing to the result.
func (acc *Accumulator) OrAcc(other *Accumulator) {
	if other == nil {
		return
	}
	acc.gen++
	n := len(other.keys)
	conc := calcConcurrency(n, minContainersPerRoutine, acc.concCap())
	if conc <= 1 {
		for i, key := range other.keys {
			// A fresh block is adopted by copy, so the zeroing would be wasted.
			dst, created := acc.stageBlock(key, true)
			if created {
				copy(dst, other.blocks[i])
				continue
			}
			d64 := uint16To64SliceUnsafe(dst)
			for j, w := range uint16To64SliceUnsafe(other.blocks[i]) {
				d64[j] |= w
			}
		}
		return
	}
	// As in Or: pre-create every block here, so workers only read the index
	// and write disjoint blocks.
	for _, key := range other.keys {
		acc.stageBlock(key, false) // orAccRange ORs, so it must be zeroed
	}
	concurrentlyInRanges(n, conc, func(from, to, _ int) {
		acc.orAccRange(other, from, to)
	})
}

// orAccRange ORs other's blocks [from, to) into their staging blocks, which
// must already exist. Reads the index without touching lastIdx and writes only
// its own blocks, so disjoint ranges may run concurrently.
func (acc *Accumulator) orAccRange(other *Accumulator, from, to int) {
	ai := -1 // so the first gallop may land on index 0
	for i := from; i < to; i++ {
		// Keys ascend, so each lookup gallops on from the last hit.
		ai = acc.searchFrom(ai, len(acc.keys), other.keys[i])
		d64 := uint16To64SliceUnsafe(acc.blocks[ai])
		for j, w := range uint16To64SliceUnsafe(other.blocks[i]) {
			d64[j] |= w
		}
	}
}

// orRange deposits bm's containers [from, to) into their staging blocks, which
// must already exist. Concurrency-safe on disjoint ranges, as orAccRange.
func (acc *Accumulator) orRange(bm *Bitmap, from, to int) {
	ai := -1 // so the first gallop may land on index 0
	for i := from; i < to; i++ {
		off := bm.keys.val(i)
		if getCardinality(bm.data[off:]) == 0 {
			continue
		}
		// Keys ascend, so each lookup gallops on from the last hit.
		ai = acc.searchFrom(ai, len(acc.keys), bm.keys.key(i))
		src := bm.getContainer(off)
		dst := acc.blocks[ai]
		switch src[indexType] {
		case typeArray:
			for _, lo := range array(src).all() {
				setBit(dst, lo)
			}
		case typeBitmap:
			d64 := uint16To64SliceUnsafe(dst)
			s64 := uint16To64SliceUnsafe(src[startIdx:maxContainerSize])
			for j, w := range s64 {
				d64[j] |= w
			}
		default:
			panic(fmt.Sprintf("Accumulator.Or: unknown container type %d", src[indexType]))
		}
	}
}

// AndNot removes all elements of bm from the accumulator. bm is only read,
// never retained: it may be released or reused as soon as AndNot returns.
// Only ranges staged on both sides do any work — no staging block is ever
// created — and the key walk gallops over whichever side is much larger,
// so subtracting a wide bitmap from a narrow accumulator, or the reverse,
// stays cheap.
func (acc *Accumulator) AndNot(bm *Bitmap) {
	if bm == nil {
		return
	}
	acc.gen++
	an, bn := len(acc.keys), bm.keys.numKeys()
	if an == 0 || bn == 0 {
		return
	}
	// Only ranges on both sides do work, so gate on the smaller side.
	conc := calcConcurrency(min(an, bn), minContainersPerRoutine, acc.concCap())
	if conc <= 1 {
		acc.andNotRange(bm, 0, an)
		return
	}
	concurrentlyInRanges(an, conc, func(from, to, _ int) {
		acc.andNotRange(bm, from, to)
	})
}

// AndNotAcc removes everything staged in other from the accumulator. The
// receiver is the destination; other is only read, never retained, and
// must stay unmodified for the duration of the call. Only ranges staged
// on both sides do any work — no staging block is ever created — and the
// key walk gallops over whichever side is much larger. AndNotAcc on
// itself empties every staged range.
func (acc *Accumulator) AndNotAcc(other *Accumulator) {
	if other == nil {
		return
	}
	acc.gen++
	an, bn := len(acc.keys), len(other.keys)
	if an == 0 || bn == 0 {
		return
	}
	// Only ranges on both sides do work, so gate on the smaller side.
	conc := calcConcurrency(min(an, bn), minContainersPerRoutine, acc.concCap())
	if conc <= 1 {
		acc.andNotAccRange(other, 0, an)
		return
	}
	concurrentlyInRanges(an, conc, func(from, to, _ int) {
		acc.andNotAccRange(other, from, to)
	})
}

// andNotAccRange clears other's staged bits from acc's blocks [from, to): the
// accumulator-source sibling of andNotRange, same walk, same concurrency
// rules.
func (acc *Accumulator) andNotAccRange(other *Accumulator, from, to int) {
	if from >= to {
		return
	}
	bn := len(other.keys)
	useGallopAcc := shouldGallop(to-from, bn)
	useGallopOther := shouldGallop(bn, to-from)

	ai := from
	bi, _ := slices.BinarySearch(other.keys, acc.keys[from])
	for ai < to && bi < bn {
		ak, bk := acc.keys[ai], other.keys[bi]
		if ak < bk {
			if useGallopAcc {
				ai = acc.searchFrom(ai, to, bk)
			} else {
				ai++
			}
			continue
		}
		if ak > bk {
			if useGallopOther {
				bi = other.searchFrom(bi, bn, ak)
			} else {
				bi++
			}
			continue
		}
		d64 := uint16To64SliceUnsafe(acc.blocks[ai])
		for j, w := range uint16To64SliceUnsafe(other.blocks[bi]) {
			d64[j] &^= w
		}
		ai++
		bi++
	}
}

// andNotRange clears bm's elements from acc's staging blocks [from, to): a
// two-pointer walk over both ascending key sets, galloping whichever side is
// much larger. Reads the index without touching lastIdx and writes only its
// own blocks, so disjoint ranges may run concurrently.
func (acc *Accumulator) andNotRange(bm *Bitmap, from, to int) {
	if from >= to {
		return
	}
	bn := bm.keys.numKeys()
	useGallopAcc := shouldGallop(to-from, bn)
	useGallopSrc := shouldGallop(bn, to-from)

	ai := from
	bi := bm.keys.search(acc.keys[from])
	for ai < to && bi < bn {
		ak, bk := acc.keys[ai], bm.keys.key(bi)
		if ak < bk {
			if useGallopAcc {
				ai = acc.searchFrom(ai, to, bk)
			} else {
				ai++
			}
			continue
		}
		if ak > bk {
			if useGallopSrc {
				bi = bm.keys.searchFrom(bi, ak)
			} else {
				bi++
			}
			continue
		}
		src := bm.getContainer(bm.keys.val(bi))
		if getCardinality(src) > 0 {
			dst := acc.blocks[ai]
			switch src[indexType] {
			case typeArray:
				for _, lo := range array(src).all() {
					clearBit(dst, lo)
				}
			case typeBitmap:
				d64 := uint16To64SliceUnsafe(dst)
				s64 := uint16To64SliceUnsafe(src[startIdx:maxContainerSize])
				for j, w := range s64 {
					d64[j] &^= w
				}
			default:
				panic(fmt.Sprintf("Accumulator.AndNot: unknown container type %d", src[indexType]))
			}
		}
		ai++
		bi++
	}
}

// layoutScratchLen is how many per-block layout entries the build path can
// hold on the caller's stack. Unions spanning more ranges pay one heap
// allocation for the slice.
const layoutScratchLen = 64

// blockLayout is one staging block's entry from the layout pass: its popcount
// and the window of words holding its set bits, so the build reads only that
// span instead of the whole 8KB block. min and max are inclusive uint16-word
// indices, meaningful only when card > 0.
type blockLayout struct {
	card     int32 // fits: a block holds at most 65536 bits
	min, max uint16
}

// layout popcounts the staging blocks into the result's complete layout — per
// block a cardinality and touched-word window — so a build can allocate fully
// sized and never reallocate or move anything. layouts is scratch[:len(blocks)]
// when it fits, a fresh slice otherwise. numKeys includes the always-present
// key-0 slot; sizeContainer0 is that container's size, a minContainerSize
// placeholder when the union misses key 0.
func (acc *Accumulator) layout(scratch []blockLayout) (layouts []blockLayout, numKeys, sizeContainer0, sizeOtherContainers int) {
	if len(acc.blocks) <= len(scratch) {
		layouts = scratch[:len(acc.blocks)]
	} else {
		layouts = make([]blockLayout, len(acc.blocks))
	}
	numKeys = 1 // the key-0 slot every bitmap pre-creates
	sizeContainer0 = minContainerSize
	for i, b := range acc.blocks {
		// Splitting the prefix/suffix zero-scan from the popcount beats
		// tracking the window inside the popcount loop, which would tax every
		// word of every block; only the two boundary words are read twice.
		b64 := uint16To64SliceUnsafe(b)
		first := 0
		for first < len(b64) && b64[first] == 0 {
			first++
		}
		if first == len(b64) {
			layouts[i] = blockLayout{}
			continue
		}
		last := len(b64) - 1
		for b64[last] == 0 { // terminates: b64[first] != 0
			last--
		}
		card := 0
		for _, x := range b64[first : last+1] {
			card += bits.OnesCount64(x)
		}
		// first/last are 64-bit-word indices; widen to the covered
		// uint16-word span.
		layouts[i] = blockLayout{card: int32(card), min: uint16(first * 4), max: uint16(last*4 + 3)}
		sz, _ := containerSizeForCard(card)
		if acc.keys[i] != 0 {
			numKeys++
			sizeOtherContainers += int(sz)
		} else {
			// Every bitmap pre-creates key 0 (the node cannot tell a zero key
			// from an empty slot), so size that container rather than count a
			// second one; buildInto fills it in place.
			sizeContainer0 = int(sz)
		}
	}
	return layouts, numKeys, sizeContainer0, sizeOtherContainers
}

// Bitmap assembles and returns the union. The accumulator keeps its staging
// state afterwards; call Reset to reuse it for a new union.
func (acc *Accumulator) Bitmap() *Bitmap {
	var scratch [layoutScratchLen]blockLayout
	layouts, numKeys, sizeContainer0, sizeOtherContainers := acc.layout(scratch[:])
	ra := initBitmapWithCap(&Bitmap{}, numKeys+1, sizeContainer0, sizeOtherContainers)
	acc.buildInto(ra, layouts)
	return ra
}

// BitmapToBuf assembles the union into a buffer obtained from get — for
// callers that pool result memory — and returns a bitmap backed by it. get
// is called exactly once, with the exact required size in bytes, and must
// not itself use the accumulator (doing so panics). Returning a buffer
// smaller than the requested size panics — the caller was told exactly how
// much is needed.
// The buffer is adopted to its full capacity (rounded down to even), not
// just its length — a length-limited view of a larger backing array hands
// the whole backing array to the result. The returned bitmap holds a
// reference to the buffer, so the buffer must not be reused until the
// bitmap is released. Mutating the result stays within the buffer's
// capacity until it needs to grow, at which point it migrates to the heap.
func (acc *Accumulator) BitmapToBuf(get func(sizeBytes int) []byte) *Bitmap {
	return acc.bitmapToBuf("Accumulator.BitmapToBuf", wrapGetToBuf(get))
}

// InitBitmapToBuf is BitmapToBuf for callers that pool the result Bitmap
// struct together with its buffer: get returns both — typically from one
// pool entry — and the union is built into them, so a warm accumulator
// building into pooled memory allocates nothing. The struct must be
// non-nil; its previous fields are overwritten, never freed — it must not
// own anything the caller still needs. A buffer smaller than the requested
// size panics, as in BitmapToBuf.
func (acc *Accumulator) InitBitmapToBuf(get func(sizeBytes int) (*Bitmap, []byte)) *Bitmap {
	return acc.bitmapToBuf("Accumulator.InitBitmapToBuf", get)
}

// bitmapToBuf is the shared body of the ToBuf builds; name is the exported
// constructor the caller reached it through, so panics name that one.
func (acc *Accumulator) bitmapToBuf(name string, get func(sizeBytes int) (*Bitmap, []byte)) *Bitmap {
	if get == nil {
		panic(name + ": get is nil")
	}
	var scratch [layoutScratchLen]blockLayout
	layouts, numKeys, sizeContainer0, sizeOtherContainers := acc.layout(scratch[:])
	// +1 is the spare slot buildInto relies on.
	sizeKeys := calcSizeKeys(numKeys + 1)
	sizeTotal := sizeKeys + sizeContainer0 + sizeOtherContainers

	gen := acc.gen
	dst, buf := get(sizeTotal * 2)
	if acc.gen != gen {
		panic(name + ": mutated during build — get must not use the accumulator")
	}
	initBitmapToBufExact(name, dst, buf, sizeKeys, sizeContainer0, sizeTotal)
	acc.buildInto(dst, layouts)
	return dst
}

// buildInto builds the result containers into ra, which must already have
// capacity for the full layout plus one spare key slot (the numKeys+1 at the
// call sites). The spare is what lets setKey's return — a possibly shifted
// offset — be discarded: the node never fills mid-build, so it never expands
// and never moves containers. acc.keys is sorted, so containers are appended
// in key order and no append can reallocate. Each block is written from its
// layout's window only; the words outside it are zeroed rather than copied,
// since the container may come from newContainerNoClr over dirty memory.
func (acc *Accumulator) buildInto(ra *Bitmap, layouts []blockLayout) {
	for i, b := range acc.blocks {
		bl := layouts[i]
		card := int(bl.card)
		if card == 0 {
			continue
		}
		sz, typ := containerSizeForCard(card)
		var off uint64
		if acc.keys[i] == 0 {
			// Pre-created at exactly sz by layout, key slot already set:
			// appending here would orphan it as dead space in ra.data.
			off = ra.keys.val(0)
		} else {
			off = ra.newContainerNoClr(sz)
			ra.data[off] = sz
		}
		c := ra.getContainer(off)
		c[indexType] = typ
		setCardinality(c, card)
		if typ == typeArray {
			// Only the touched window holds set bits — skip the rest.
			bitsIntoArray(b[bl.min:bl.max+1], c[startIdx:], bl.min)
			// Lookups never read the padding, but ToBuffer serializes it —
			// clear it so a dirty pooled buffer cannot leak into the bytes.
			clear(c[int(startIdx)+card:])
		} else {
			// Only the window holds set bits, but newContainerNoClr can hand
			// back dirty memory, so the rest is zeroed rather than copied.
			lo, hi := int(startIdx)+int(bl.min), int(startIdx)+int(bl.max)
			clear(c[startIdx:lo])
			copy(c[lo:], b[bl.min:bl.max+1])
			clear(c[hi+1:])
		}
		if acc.keys[i] != 0 {
			ra.setKey(acc.keys[i], off)
		}
	}
}

// retainedSlotFactor bounds the keys/blocks/free capacities kept across Reset,
// as a multiple of WithRetainedBlocks. A slot costs 56 bytes against a block's
// 8KB, so the slack is ~2%; a tighter bound would hand the slices back on every
// union, since append overshoots the budget while building them — by up to
// ~2.3x for [][]uint16.
const retainedSlotFactor = 3

// Reset clears the accumulator for reuse. Up to WithRetainedBlocks staging
// blocks are kept as key-independent spares for the next union's ranges,
// whatever they are; everything else is released, so a pooled accumulator's
// memory and per-union cost stay bounded by the current union, not by every
// range it has ever served. Retained blocks are kept dirty and zeroed only
// when a later union actually claims one (insertBlockAt), so a spare that is
// dropped before reuse costs no clear. Reset bounds retained memory only —
// the peak during a union is whatever its spread demands.
func (acc *Accumulator) Reset() {
	acc.gen++
	// Bound free before refilling it, so the top-up never writes into an array
	// about to be handed back. Only a lowered WithRetainedBlocks gets here:
	// free never outgrows n on its own.
	n := acc.maxRetained
	slots := retainedSlotFactor * n
	if cap(acc.free) > slots {
		// Rebuilding at the budget releases the array and, with it, the spares
		// past n — nothing else references their blocks.
		free := make([][]uint16, min(len(acc.free), n), n)
		copy(free, acc.free)
		acc.free = free
	}
	if flen := len(acc.free); flen < n {
		if blen := len(acc.blocks); blen > 0 {
			// Spares are kept dirty; insertBlockAt zeroes one when it is claimed.
			acc.free = append(acc.free, acc.blocks[:min(n-flen, blen)]...)
		}
	} else if flen > n {
		// Array is within bound, only the spares are surplus: no need to copy.
		clear(acc.free[n:])
		acc.free = acc.free[:n]
	}

	// keys and blocks are bounded the same way but judged separately: they grow
	// on different size classes, so one can be over while the other is not.
	if cap(acc.keys) > slots {
		acc.keys = nil
	} else {
		acc.keys = acc.keys[:0]
	}
	if cap(acc.blocks) > slots {
		acc.blocks = nil
	} else {
		clear(acc.blocks) // a bare [:0] would leave the dropped blocks reachable
		acc.blocks = acc.blocks[:0]
	}
	acc.lastIdx = 0
}
