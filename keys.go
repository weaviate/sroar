package sroar

import (
	"fmt"
	"strings"
)

var (
	indexNodeSize  = 0
	indexNumKeys   = 1
	indexNodeStart = 2
)

// node stores uint64 keys and the corresponding container offset in the buffer.
// 0th index (indexNodeSize) is used for storing the size of node in bytes.
// 1st index (indexNumKeys) is used for storing the number of keys.
// 2nd index is where we start writing the key-value pairs.
type node []uint64

func keyOffset(i int) int { return indexNodeStart + 2*i }
func valOffset(i int) int { return indexNodeStart + 2*i + 1 }

func (n node) numKeys() int        { return int(n[indexNumKeys]) }
func (n node) size() int           { return int(n[indexNodeSize]) }
func (n node) maxKeys() int        { return (len(n) - indexNodeStart) / 2 }
func (n node) key(i int) uint64    { return n[keyOffset(i)] }
func (n node) val(i int) uint64    { return n[valOffset(i)] }
func (n node) data(i int) []uint64 { return n[keyOffset(i):keyOffset(i+1)] }

func (n node) uint64(idx int) uint64   { return n[idx] }
func (n node) setAt(idx int, k uint64) { n[idx] = k }

func (n node) setNumKeys(num int) { n[indexNumKeys] = uint64(num) }
func (n node) setNodeSize(sz int) { n[indexNodeSize] = uint64(sz) }

func (n node) maxKey() uint64 {
	idx := n.numKeys()
	// numKeys == index of the max key, because 0th index is being used for meta information.
	if idx == 0 {
		return 0
	}
	return n.key(idx)
}

func (n node) moveRight(lo int) {
	hi := n.numKeys()
	assert(!n.isFull())
	// copy works despite of overlap in src and dst.
	// See https://golang.org/pkg/builtin/#copy
	copy(n[keyOffset(lo+1):keyOffset(hi+1)], n[keyOffset(lo):keyOffset(hi)])
}

// isFull checks that the node is already full.
func (n node) isFull() bool {
	return n.numKeys() == n.maxKeys()
}

// Search returns the index of a smallest key >= k in a node.
func (n node) search(k uint64) int {
	N := n.numKeys()
	if N == 0 {
		return 0
	}
	// keys and offsets are interleaved (keys at even indices). Slice the key
	// region so indexing is keys[2*i]; cheaper midpoint and a single load per
	// step. The first key is in the first cache line and always hot: this also
	// handles k before the first key / an exact match at position 0.
	keys := n[indexNodeStart : indexNodeStart+2*N]
	if k <= keys[0] {
		return 0
	}
	lo, hi := 0, N-1
	for lo+8 <= hi {
		mid := int(uint(lo+hi) / 2)
		ki := keys[2*mid]
		if ki < k {
			lo = mid + 1
		} else if ki > k {
			hi = mid
		} else {
			return mid
		}
	}
	for ; lo <= hi; lo++ {
		if keys[2*lo] >= k {
			return lo
		}
	}
	return N
}

// searchFrom returns the index of the smallest key >= k starting the search
// at position from. Uses exponential search to bracket the target then binary
// search to pinpoint it — O(log gap) where gap is the distance from from to
// the result. Equivalent to bi++ for gap=1, but much faster for large gaps.
func (n node) searchFrom(from int, k uint64) int {
	N := n.numKeys()
	lower := from + 1
	if lower >= N || n.key(lower) >= k {
		return lower
	}
	// Exponential expansion to bracket k.
	span := 1
	for lower+span < N && n.key(lower+span) < k {
		span *= 2
	}
	upper := lower + span
	if upper >= N {
		upper = N - 1
	}
	if n.key(upper) < k {
		return N
	}
	// Binary search within [lower + span/2, upper].
	lower += span >> 1
	for lower+1 < upper {
		mid := (lower + upper) >> 1
		ki := n.key(mid)
		if ki < k {
			lower = mid
		} else if ki > k {
			upper = mid
		} else {
			return mid
		}
	}
	return upper
}

// Search returns the index of a smallest key >= k in a node.
// Runs from highest to smallest key.
func (n node) searchReverse(k uint64) int {
	N := n.numKeys()
	idx := N

	for i := N - 1; i >= 0; i-- {
		if n.key(i) < k {
			break
		}
		idx = i
	}
	return idx
}

func zeroOut(data []uint64) {
	for i := 0; i < len(data); i++ {
		data[i] = 0
	}
}

// compacts the node i.e., remove all the kvs with value < lo. It returns the remaining number of
// keys.
func (n node) compact(lo uint64) int {
	N := n.numKeys()
	mk := n.maxKey()
	var left, right int
	for right = 0; right < N; right++ {
		if n.val(right) < lo && n.key(right) < mk {
			// Skip over this key. Don't copy it.
			continue
		}
		// Valid data. Copy it from right to left. Advance left.
		if left != right {
			copy(n.data(left), n.data(right))
		}
		left++
	}
	// zero out rest of the kv pairs.
	zeroOut(n[keyOffset(left):keyOffset(right)])
	n.setNumKeys(left)

	// If the only key we have is the max key, and its value is less than lo, then we can indicate
	// to the caller by returning a zero that it's OK to drop the node.
	if left == 1 && n.key(0) == mk && n.val(0) < lo {
		return 0
	}
	return left
}

// getValue returns the value corresponding to the key if found.
func (n node) getValue(k uint64) (uint64, bool) {
	k &= mask // Ensure k has its lowest bits unset.
	idx := n.search(k)
	// key is not found
	if idx >= n.numKeys() {
		return 0, false
	}
	if ki := n.key(idx); ki == k {
		return n.val(idx), true
	}
	return 0, false
}

// set returns true if it added a new key.
func (n node) set(k, v uint64) bool {
	N := n.numKeys()
	idx := n.search(k)
	if idx == N {
		n.setNumKeys(N + 1)
		n.setAt(keyOffset(idx), k)
		n.setAt(valOffset(idx), v)
		return true
	}

	ki := n.key(idx)
	if N == n.maxKeys() {
		// This happens during split of non-root node, when we are updating the child pointer of
		// right node. Hence, the key should already exist.
		assert(ki == k)
	}
	if ki == k {
		n.setAt(valOffset(idx), v)
		return false
	}
	assert(ki > k)
	// Found the first entry which is greater than k. So, we need to fit k
	// just before it. For that, we should move the rest of the data in the
	// node to the right to make space for k.
	n.moveRight(idx)
	n.setNumKeys(N + 1)
	n.setAt(keyOffset(idx), k)
	n.setAt(valOffset(idx), v)
	return true
	// panic("shouldn't reach here")
}

// updateOffsets shifts every container offset greater than beyond by `by`
// (added when add is true, subtracted otherwise). It is used when a container
// is expanded or removed in place and all containers physically after it move.
//
// The offset column is interleaved with keys (stride 2) and sorted by key, not
// by offset, so every key must be visited — the scan is inherently O(numKeys).
// The loop is hoisted to a single bounds-checked slice with the add/sub branch
// lifted out of the loop body.
func (n node) updateOffsets(beyond, by uint64, add bool) {
	vals := n[indexNodeStart : indexNodeStart+2*n.numKeys()]
	if add {
		for i := 1; i < len(vals); i += 2 {
			if o := vals[i]; o > beyond {
				vals[i] = o + by
			}
		}
	} else {
		for i := 1; i < len(vals); i += 2 {
			if o := vals[i]; o > beyond {
				assert(o >= by)
				vals[i] = o - by
			}
		}
	}
}

// updateAllOffsets adds delta to every container offset stored in the node.
// It is used when the key region grows and all offsets must be shifted right.
func (n node) updateAllOffsets(delta uint64) {
	vals := n[indexNodeStart : indexNodeStart+2*n.numKeys()]
	for i := 1; i < len(vals); i += 2 {
		vals[i] += delta
	}
}

func (n node) iterate(fn func(node, int)) {
	for i := 0; i < n.maxKeys(); i++ {
		if k := n.key(i); k > 0 {
			fn(n, i)
		} else {
			break
		}
	}
}

func (n node) print(parentID uint64) {
	var keys []string
	n.iterate(func(n node, i int) {
		keys = append(keys, fmt.Sprintf("%d", n.key(i)))
	})
	if len(keys) > 8 {
		copy(keys[4:], keys[len(keys)-4:])
		keys[3] = "..."
		keys = keys[:8]
	}
	fmt.Printf("num keys: %d keys: %s\n", n.numKeys(), strings.Join(keys, " "))
}
