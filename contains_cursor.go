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
		i := cur.arrayGeqPos(y)
		cur.arrPos = i
		return i < cur.arrN && cur.lastContainer[int(startIdx)+i] == y
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
		cur.arrPos = cur.arrN // forces arrayGeqPos to search from scratch
		i := cur.arrayGeqPos(y)
		cur.arrPos = i
		return i < cur.arrN && cur.lastContainer[int(startIdx)+i] == y
	default:
		// mirror Contains' default-false on unknown container types; nil cont
		// makes repeat probes into this container report false as well.
		cur.lastContainer = nil
		return false
	}
}

// arrayGeqPos returns the index of the smallest value >= y in the cached array
// container (arrN if none). arrPos holds the lower bound of the previous probe,
// so everything left of it is smaller than any forward probe: a y at the cursor
// or in the gap just below it resolves in O(1), a forward move advances in
// O(log(distance moved)), a true backward move binary-searches from scratch.
func (cur *ContainsCursor) arrayGeqPos(y uint16) int {
	c, si, i := cur.lastContainer, int(startIdx), cur.arrPos
	switch {
	case i < cur.arrN && c[si+i] < y:
		return advanceUntil(c[si:], i, cur.arrN, y)
	case i == 0 || c[si+i-1] < y:
		// before the first value (i == 0), between the neighbours
		// (c[i-1] < y <= c[i]), or past the last value (i == arrN and
		// c[arrN-1] < y): i is already y's lower bound
		return i
	default:
		return array(c).find(y)
	}
}

// NextGeq returns the smallest set value >= x and whether one exists. It shares
// the cursor's cache and leaves the cursor positioned on the returned value, so
// interleaving Contains and NextGeq with non-decreasing arguments keeps both on
// their fast paths. This is the successor primitive for leapfrog intersection:
// instead of probing candidates one by one, a caller can jump directly to the
// bitmap's next admissible value.
func (cur *ContainsCursor) NextGeq(x uint64) (uint64, bool) {
	if cur == nil || cur.ra == nil {
		return 0, false
	}
	key := x & mask
	y := uint16(x)
	var idx int
	switch {
	case key == cur.lastKey:
		// same-key fast path: resolve within the cached container without
		// touching the keys node or the arena. Skipping the maxKey check is
		// sound — lastKey is a resolved key (<= maxKey) or the unmatchable
		// sentinel.
		if cur.lastContainer == nil {
			idx = cur.lastIdx // key resolved as absent: walk from its slot
			break
		}
		if cur.isLastBitmap {
			if v, ok := bitmap(cur.lastContainer).nextGeq(y); ok {
				return key | uint64(v), true
			}
		} else if pos := cur.arrayGeqPos(y); pos < cur.arrN {
			cur.arrPos = pos
			return key | uint64(cur.lastContainer[int(startIdx)+pos]), true
		}
		idx = cur.lastIdx + 1 // cached container exhausted: successor is a later container's min
	case key > cur.maxKey:
		return 0, false
	case key > cur.lastKey:
		idx = cur.lastIdx // forward: gallop from where we are
		if idx < cur.numKeys && cur.keys.key(idx) < key {
			idx = cur.keys.searchFrom(idx, key)
		}
	default:
		idx = cur.keys.search(key) // backward: plain search
	}
	// walk containers forward until one holds a value at or after x; for
	// containers past x's own, any set value qualifies (want == 0)
	for ; idx < cur.numKeys; idx++ {
		ck := cur.keys.key(idx)
		c := cur.ra.getContainer(cur.keys.val(idx))
		n := getCardinality(c)
		if n == 0 {
			continue
		}
		want := uint16(0)
		if ck == key {
			want = y
		}
		switch c[indexType] {
		case typeBitmap:
			if v, ok := bitmap(c).nextGeq(want); ok {
				cur.lastIdx, cur.lastKey = idx, ck
				cur.lastContainer, cur.isLastBitmap = c, true
				return ck | uint64(v), true
			}
		case typeArray:
			pos := 0
			if want > 0 {
				pos = array(c).find(want)
			}
			if pos < n {
				cur.lastIdx, cur.lastKey = idx, ck
				cur.lastContainer, cur.isLastBitmap = c, false
				cur.arrN, cur.arrPos = n, pos
				return ck | uint64(c[int(startIdx)+pos]), true
			}
		}
		// unknown container types are skipped, mirroring Contains' default-false
	}
	// no set value at or after x; cursor state is left untouched
	return 0, false
}
