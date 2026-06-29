package sroar

import "testing"

// Regression: RemoveRange with a lo past the last element of a *full* array
// container used to read one past the container's end (array.removeRange
// dereferenced c[startIdx+loIdx] before checking loIdx == N), panicking with an
// index-out-of-range. The panic needs a container with no slack, i.e.
// startIdx+cardinality == container size. Found by randomized RemoveRange fuzzing.
func TestRemoveRangePastEndOfFullContainer(t *testing.T) {
	// key is the high 48 bits of a value (the container selector); elements are
	// key|low.
	const key = uint64(14) << 16

	// Fill one key's array container exactly to capacity (full, no slack) with
	// ascending lows, all below the range we will remove.
	full := uint64(minContainerSize - int(startIdx))
	ra := NewBitmap()
	for low := uint64(0); low < full; low++ {
		ra.Set(key | low)
	}
	off, _ := ra.keys.getValue(key)
	if int(ra.getContainer(off)[indexSize]) != minContainerSize {
		t.Fatalf("container not full as expected (size %d, want %d); adjust the fill count",
			ra.getContainer(off)[indexSize], minContainerSize)
	}

	// Remove a range entirely above every stored element. lo is past the last
	// element, so find(lo) == cardinality. Must be a no-op, not a panic.
	ra.RemoveRange(key|50000, key|60000)
	for low := uint64(0); low < full; low++ {
		if !ra.Contains(key | low) {
			t.Fatalf("element %d wrongly removed by out-of-range RemoveRange", low)
		}
	}
	if got := ra.GetCardinality(); got != int(full) {
		t.Fatalf("cardinality changed by no-op RemoveRange: got %d want %d", got, full)
	}
}
