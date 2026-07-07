package sroar

// ContainsCursor accelerates repeated Contains on a fixed Bitmap when successive
// probes have locality: it returns the identical result to Bitmap.Contains for any
// x in any order, and is cheapest when probes repeat the previous container or
// advance past it (same-container repeats skip the key search; forward moves
// gallop from the cached position at both the container and the array-value
// level; backward moves fall back to the plain search).
//
// Not safe for concurrent use; the bound Bitmap must not be mutated while the
// cursor is live.
type ContainsCursor struct {
	ra      *Bitmap
	keys    node
	numKeys int
	maxKey  uint64 // largest container key in ra: probes above it short-circuit
	lastIdx int    // smallest key index whose key is >= lastKey
	// lastKey is the previous probe's key (x & mask, so its low 16 bits are
	// always zero). Reset seeds it with 1 — not a representable key — so a fresh
	// cursor can never false-hit the same-key cache, and idx = 0 is a valid
	// forward starting point for any first probe.
	lastKey       uint64
	lastContainer []uint16
	arrN          int  // array cardinality (valid when lastContainer != nil && !isLastBitmap)
	arrPos        int  // lower bound of the last probed value within the array
	isLastBitmap  bool // lastContainer is a bitmap (not array) container
}

// NewContainsCursor returns a cursor bound to ra. A nil ra (or nil cursor) reports
// false, matching Bitmap.Contains.
func (ra *Bitmap) NewContainsCursor() *ContainsCursor {
	cur := &ContainsCursor{}
	cur.Reset(ra)
	return cur
}

// Reset re-binds the cursor to ra and clears its cache, letting callers embed a
// ContainsCursor by value and prime it without a heap allocation.
func (cur *ContainsCursor) Reset(ra *Bitmap) {
	*cur = ContainsCursor{ra: ra, lastKey: 1}
	if ra != nil {
		cur.keys = ra.keys
		cur.numKeys = cur.keys.numKeys()
		if cur.numKeys > 0 {
			cur.maxKey = cur.keys.key(cur.numKeys - 1)
		}
	}
}

// Contains reports whether x is set, for any x in any order (cheapest when x
// repeats the previous container or advances past it).
func (cur *ContainsCursor) Contains(x uint64) bool {
	if cur == nil || cur.ra == nil {
		return false
	}
	key := x & mask
	y := uint16(x)
	if key == cur.lastKey {
		if cur.lastContainer == nil {
			return false
		}
		if cur.isLastBitmap {
			return bitmap(cur.lastContainer).has(y)
		}
		return cur.arrayHas(y)
	}
	if key > cur.maxKey {
		// beyond the last container: absent, and no cursor state to update —
		// turns backward probes above the range from a full search into one
		// compare (placed after the same-key path to keep cache hits untouched).
		return false
	}
	var idx int
	if key > cur.lastKey {
		idx = cur.lastIdx // forward: gallop from where we are
		if idx < cur.numKeys && cur.keys.key(idx) < key {
			idx = cur.keys.searchFrom(idx, key)
		}
	} else {
		idx = cur.keys.search(key) // backward: plain search
	}
	cur.lastIdx = idx
	cur.lastKey = key
	if idx >= cur.numKeys || cur.keys.key(idx) != key {
		cur.lastContainer = nil
		return false
	}
	cur.lastContainer = cur.ra.getContainer(cur.keys.val(idx))
	switch cur.lastContainer[indexType] {
	case typeBitmap:
		cur.isLastBitmap = true
		return bitmap(cur.lastContainer).has(y)
	case typeArray:
		cur.isLastBitmap = false
		cur.arrN = getCardinality(cur.lastContainer)
		cur.arrPos = cur.arrN // arrayHas re-establishes the lower bound
		return cur.arrayHas(y)
	default:
		// mirror Contains' default-false on unknown container types; nil cont
		// makes repeat probes into this container report false as well.
		cur.lastContainer = nil
		return false
	}
}

// arrayHas answers has(y) for the cached array container.
//
// Invariant: arrPos is the lower bound of the previous probe — the index of the
// smallest value >= it, arrN when the probe was past the last value. Example
// with values c = [10 20 30 40] after a previous probe of 25 (arrPos = 2,
// c[2] = 30):
//
//	y = 30          hit under the cursor          -> true, no search
//	y = 31..39      forward of the cursor         -> advanceUntil from index 2
//	y = 21..29      inside the gap (20, 30)       -> absent, one compare (20 < y)
//	y <= 20         at or behind the previous gap -> full binary search
//
// Only the last shape searches: the invariant already brackets y in the other
// three. The single-gap check cannot be extended to earlier gaps
// (c[i-2]..c[i-1] and so on) — locating which earlier gap holds y IS the
// binary search that find() performs. Nor are explicit y < c[0] / y > c[N-1]
// boundary cases needed: both are answered below via the cursor's neighbours
// (already in cache) instead of re-reading the array edges.
func (cur *ContainsCursor) arrayHas(y uint16) bool {
	c, si, i := cur.lastContainer, int(startIdx), cur.arrPos
	switch {
	case i < cur.arrN && c[si+i] == y:
		// the cursor already sits on y
		return true
	case i < cur.arrN && c[si+i] < y:
		// forward of the cursor: advance, O(log(distance moved))
		i = advanceUntil(c[si:], i, cur.arrN, y)
	case i == 0:
		// cursor at the start and c[0] > y (or the container is empty):
		// y precedes every value
		return false
	case c[si+i-1] < y:
		// inside the gap right below the cursor (c[i-1] < y < c[i], or past
		// the last value when i == arrN): provably absent, and arrPos is
		// already y's lower bound
		return false
	default:
		// at or behind the previous gap: full binary search
		i = array(c).find(y)
	}
	cur.arrPos = i
	return i < cur.arrN && c[si+i] == y
}
