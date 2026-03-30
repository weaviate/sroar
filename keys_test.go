package sroar

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func makeNodeForSearch(numKeys int) node {
	bm := NewBitmap()
	for i := 0; i < numKeys; i++ {
		bm.Set(uint64(i+1) << 16)
	}
	return bm.keys
}

func TestSearchFrom(t *testing.T) {
	t.Run("gap of 1 — returns from+1 immediately", func(t *testing.T) {
		n := makeNodeForSearch(10)
		result := n.searchFrom(0, n.key(1))
		require.Equal(t, 1, result)
	})

	t.Run("exact match within range", func(t *testing.T) {
		n := makeNodeForSearch(100)
		for _, from := range []int{0, 10, 50} {
			for target := from + 1; target < n.numKeys(); target++ {
				k := n.key(target)
				got := n.searchFrom(from, k)
				require.Equal(t, target, got, "from=%d target=%d k=%d", from, target, k)
			}
		}
	})

	t.Run("between two keys returns first key >= k", func(t *testing.T) {
		n := makeNodeForSearch(50)
		between := n.key(5) + 1
		got := n.searchFrom(4, between)
		require.Equal(t, 6, got)
	})

	t.Run("k beyond all keys returns numKeys", func(t *testing.T) {
		n := makeNodeForSearch(20)
		beyond := n.key(n.numKeys()-1) + 1
		got := n.searchFrom(0, beyond)
		require.Equal(t, n.numKeys(), got)
	})

	t.Run("from at last position with k beyond returns numKeys", func(t *testing.T) {
		n := makeNodeForSearch(10)
		last := n.numKeys() - 1
		beyond := n.key(last) + 1
		got := n.searchFrom(last, beyond)
		require.Equal(t, n.numKeys(), got)
	})

	t.Run("from+1 >= numKeys returns numKeys", func(t *testing.T) {
		n := makeNodeForSearch(5)
		last := n.numKeys() - 1
		got := n.searchFrom(last, n.key(last)+1)
		require.Equal(t, n.numKeys(), got)
	})

	t.Run("large gap — exponential search path exercised", func(t *testing.T) {
		n := makeNodeForSearch(1000)
		target := n.numKeys() - 1
		k := n.key(target)
		got := n.searchFrom(0, k)
		require.Equal(t, target, got)
	})

	t.Run("large gap — between two distant keys", func(t *testing.T) {
		n := makeNodeForSearch(1000)
		between := n.key(900) + 1
		got := n.searchFrom(0, between)
		require.Equal(t, 901, got)
	})

	t.Run("agrees with search across all positions and targets", func(t *testing.T) {
		n := makeNodeForSearch(200)
		for from := 0; from < n.numKeys()-1; from++ {
			for _, offset := range []int{1, 2, 8, 16, 64, 128} {
				target := from + offset
				if target >= n.numKeys() {
					break
				}
				k := n.key(target)
				got := n.searchFrom(from, k)
				expected := n.search(k)
				require.Equal(t, expected, got, "from=%d offset=%d k=%d", from, offset, k)
			}
			if from+2 < n.numKeys() {
				between := n.key(from+1) + 1
				if between < n.key(from+2) {
					got := n.searchFrom(from, between)
					expected := n.search(between)
					require.Equal(t, expected, got, "from=%d between=%d", from, between)
				}
			}
		}
	})
}
