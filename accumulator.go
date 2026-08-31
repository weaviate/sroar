package sroar

import (
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
// Total cost is O(total input elements + touched key ranges); staging
// memory is one 8KB block per touched key range (proportional to the
// spread of the result, not to the number of sources). In the sparse
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
// Or never retains its argument, so callers may release or reuse a source's
// memory as soon as Or returns. The zero Accumulator is ready to use;
// NewAccumulator is equivalent. An Accumulator is not safe for concurrent
// use.
type Accumulator struct {
	// keys holds the touched 64K-range keys in ascending order; blocks is
	// parallel to it. Each block is a headerless 4096-uint16 payload — the
	// 64K bits of its range in bitmap-container bit order (bitmapMask), but
	// without the container metadata, which only the final containers need.
	// No cardinality is maintained during Or; it is computed once (via
	// popcount) when the result is built.
	keys   []uint64
	blocks [][]uint16

	// free holds cleared spare blocks carried across Reset (at most
	// maxRetainedBlocks). Unlike blocks, entries are not bound to any key:
	// the next union's ranges — whatever they are — claim them before
	// allocating anew.
	free [][]uint16

	// lastIdx caches the most recently hit index into keys/blocks: sources
	// frequently cluster in one range (and single-container sources under a
	// small doc-ID universe hit very few ranges), so checking it first
	// skips the binary search on runs of same-range deposits.
	lastIdx int

	// gen counts mutations (Or, Reset). Builds compare it across the get
	// callback to catch a get that uses the accumulator mid-build — the
	// layout snapshot would no longer match the staged bits.
	gen uint64
}

func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

// block returns the staging container for the 64K range of key, creating it
// on first touch.
func (acc *Accumulator) block(key uint64) []uint16 {
	if acc.lastIdx < len(acc.keys) && acc.keys[acc.lastIdx] == key {
		return acc.blocks[acc.lastIdx]
	}

	lo, found := slices.BinarySearch(acc.keys, key)
	if found {
		acc.lastIdx = lo
		return acc.blocks[lo]
	}

	// New range: insert at lo, keeping keys sorted. Ranges are few (they
	// track the spread of the union, not the number of sources), so the
	// insertion copy is rare and small.
	var b []uint16
	if n := len(acc.free) - 1; n >= 0 {
		// Take a spare; nil the vacated slot so the backing array never
		// keeps a claimed block reachable on its own.
		b, acc.free[n], acc.free = acc.free[n], nil, acc.free[:n]
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
	return b
}

// Or deposits all elements of bm into the accumulator. bm is only read,
// never retained: it may be released or reused as soon as Or returns.
func (acc *Accumulator) Or(bm *Bitmap) {
	if bm == nil {
		return
	}
	acc.gen++
	n := bm.keys.numKeys()
	for i := 0; i < n; i++ {
		src := bm.getContainer(bm.keys.val(i))
		if getCardinality(src) == 0 {
			continue
		}
		dst := acc.block(bm.keys.key(i))
		switch src[indexType] {
		case typeArray:
			for _, lo := range array(src).all() {
				dst[lo>>4] |= bitmapMask[lo&0xF]
			}
		case typeBitmap:
			d64 := uint16To64SliceUnsafe(dst)
			s64 := uint16To64SliceUnsafe(src[startIdx:maxContainerSize])
			for j, w := range s64 {
				d64[j] |= w
			}
		default:
			panic("Accumulator.Or: unknown container type")
		}
	}
}

// layoutScratchLen is how many per-block cardinalities the build path can
// hold on the caller's stack. Unions spanning more ranges pay one heap
// allocation for the slice.
const layoutScratchLen = 64

// layout runs the popcount pass over the staging blocks, determining the
// result's complete layout — per-block cardinalities and the derived key
// count and container space needs — so builds can allocate fully sized up
// front and never reallocate or move anything. cards is
// scratch[:len(blocks)] when it fits, a fresh slice otherwise. numKeys
// counts the always-present key-0 slot up front; sizeContainer0 is the
// pre-created key-0 container's size — exact when the union touches key
// 0, a minContainerSize placeholder otherwise; sizeOtherContainers covers
// all remaining containers.
func (acc *Accumulator) layout(scratch []int) (cards []int, numKeys, sizeContainer0, sizeOtherContainers int) {
	if len(acc.blocks) <= len(scratch) {
		cards = scratch[:len(acc.blocks)]
	} else {
		cards = make([]int, len(acc.blocks))
	}
	numKeys = 1 // the key-0 slot every bitmap pre-creates
	sizeContainer0 = minContainerSize
	for i, b := range acc.blocks {
		card := 0
		for _, w := range uint16To64SliceUnsafe(b) {
			card += bits.OnesCount64(w)
		}
		cards[i] = card
		if card == 0 {
			continue
		}
		sz, _ := containerSizeForCard(card)
		if acc.keys[i] != 0 {
			numKeys++
			sizeOtherContainers += int(sz)
		} else {
			// Key 0 is special-cased: every fresh bitmap pre-creates its
			// key-0 slot and container (the node cannot tell a zero key
			// from an empty slot), so its slot is already counted and,
			// instead of appending a duplicate container, the pre-created
			// one is made exactly this size and filled in place by
			// buildInto.
			sizeContainer0 = int(sz)
		}
	}
	return cards, numKeys, sizeContainer0, sizeOtherContainers
}

// Bitmap assembles and returns the union. The accumulator keeps its staging
// state afterwards; call Reset to reuse it for a new union.
func (acc *Accumulator) Bitmap() *Bitmap {
	var scratch [layoutScratchLen]int
	cards, numKeys, sizeContainer0, sizeOtherContainers := acc.layout(scratch[:])
	ra := initBitmapWithCap(&Bitmap{}, numKeys+1, sizeContainer0, sizeOtherContainers)
	acc.buildInto(ra, cards)
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
	return acc.InitBitmapToBuf(func(sizeBytes int) (*Bitmap, []byte) {
		return &Bitmap{}, get(sizeBytes)
	})
}

// InitBitmapToBuf is BitmapToBuf for callers that pool the result Bitmap
// struct together with its buffer: get returns both — typically from one
// pool entry — and the union is built into them, so a warm accumulator
// building into pooled memory allocates nothing. The struct must be
// non-nil; its previous fields are overwritten, never freed — it must not
// own anything the caller still needs. A buffer smaller than the requested
// size panics, as in BitmapToBuf.
func (acc *Accumulator) InitBitmapToBuf(get func(sizeBytes int) (*Bitmap, []byte)) *Bitmap {
	var scratch [layoutScratchLen]int
	cards, numKeys, sizeContainer0, sizeOtherContainers := acc.layout(scratch[:])
	// +1 is the spare slot buildInto relies on.
	sizeKeys := calcSizeKeys(numKeys + 1)
	sizeTotal := sizeKeys + sizeContainer0 + sizeOtherContainers

	gen := acc.gen
	dst, buf := get(sizeTotal * 2)
	if acc.gen != gen {
		panic("Accumulator: mutated during build — get must not use the accumulator")
	}
	initBitmapToBufExact("Accumulator.InitBitmapToBuf", dst, buf, sizeKeys, sizeContainer0, sizeTotal)
	acc.buildInto(dst, cards)
	return dst
}

// buildInto builds the result containers into ra, whose data array must
// already have capacity for the full layout and whose keys node must be
// sized with one spare slot beyond the result's keys (the numKeys+1 at the
// call sites): setKey's return (a possibly shifted offset) is discarded
// below, which is safe only because the spare keeps the node from ever
// filling mid-build, so setKey never expands the node or moves containers.
// Containers are built in ascending key order (acc.keys is sorted), so
// each is appended once and never moved, and no append can reallocate.
func (acc *Accumulator) buildInto(ra *Bitmap, cards []int) {
	for i, b := range acc.blocks {
		card := cards[i]
		if card == 0 {
			continue
		}
		sz, typ := containerSizeForCard(card)
		var off uint64
		if acc.keys[i] == 0 {
			// Key 0's container pre-exists at exactly sz (layout sized it
			// via sizeContainer0) with its key slot already set — fill it in
			// place; appending would orphan it as dead space in ra.data.
			off = ra.keys.val(0)
		} else {
			off = ra.newContainerNoClr(sz)
			ra.data[off] = sz
		}
		c := ra.getContainer(off)
		c[indexType] = typ
		setCardinality(c, card)
		if typ == typeArray {
			bitsIntoArray(b, c[startIdx:])
			// The padding tail is never read by lookups, but ToBuffer
			// serializes the whole data array — clear it so a dirty pooled
			// buffer cannot leak into the serialized bytes.
			clear(c[int(startIdx)+card:])
		} else {
			copy(c[startIdx:], b)
		}
		if acc.keys[i] != 0 {
			ra.setKey(acc.keys[i], off)
		}
	}
}

// maxRetainedBlocks bounds the staging blocks Reset carries over to the
// next union as spares: enough to serve typical pooled reuse without
// re-allocating, while capping a long-lived accumulator's footprint (and
// Reset's clearing cost) at maxRetainedBlocks * 8KB no matter how many
// ranges past unions touched.
const maxRetainedBlocks = 16

// maxRetainedSlots bounds the capacity of the keys/blocks slices kept
// across Reset. Their per-entry cost is tiny next to the 8KB blocks, but a
// single union over an abnormal number of ranges would otherwise pin its
// full-size backing arrays for the accumulator's lifetime.
const maxRetainedSlots = 1024

// Reset clears the accumulator for reuse. Up to maxRetainedBlocks staging
// blocks are cleared and kept as key-independent spares for the next
// union's ranges, whatever they are; everything else is released, so a
// pooled accumulator's memory and per-union cost stay bounded by the
// current union, not by every range it has ever served. Reset bounds
// retained memory only — the peak during a union is whatever its spread
// demands.
func (acc *Accumulator) Reset() {
	acc.gen++
	if acc.free == nil {
		acc.free = make([][]uint16, 0, maxRetainedBlocks)
	}
	for _, b := range acc.blocks {
		if len(acc.free) == maxRetainedBlocks {
			break
		}
		clear(b)
		acc.free = append(acc.free, b)
	}
	if cap(acc.keys) > maxRetainedSlots {
		acc.keys, acc.blocks = nil, nil
	} else {
		// Zero the pointer slots before truncating: a bare [:0] would keep
		// every dropped block reachable through the backing array.
		clear(acc.blocks)
		acc.keys = acc.keys[:0]
		acc.blocks = acc.blocks[:0]
	}
	acc.lastIdx = 0
}
