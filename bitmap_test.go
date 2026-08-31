package sroar

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	assert_ "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fill(c []uint16, b uint16) {
	for i := range c[startIdx:] {
		c[i+int(startIdx)] = b
	}
}

func TestModify(t *testing.T) {
	data := make([]uint16, 16)
	s := uint16To64SliceUnsafe(data)
	for i := 0; i < len(s); i++ {
		s[i] = uint64(i)
	}

	o := uint16To64SliceUnsafe(data)
	for i := 0; i < len(o); i++ {
		require.Equal(t, uint64(i), o[i])
	}
}

func TestNewBitmapToBuf(t *testing.T) {
	t.Run("basic set and contains", func(t *testing.T) {
		buf := make([]byte, 4096)
		bm := NewBitmapToBuf(buf)

		bm.Set(1)
		bm.Set(100)
		bm.Set(1000)

		require.Equal(t, 3, bm.GetCardinality())
		require.True(t, bm.Contains(1))
		require.True(t, bm.Contains(100))
		require.True(t, bm.Contains(1000))
	})

	t.Run("uses provided buffer memory", func(t *testing.T) {
		buf := make([]byte, 4096)
		bm := NewBitmapToBuf(buf)
		bm.Set(42)

		// The bitmap's data should be backed by buf: the buffer
		// should no longer be all zeros after a Set.
		allZero := true
		for _, b := range buf {
			if b != 0 {
				allZero = false
				break
			}
		}
		require.False(t, allZero)
	})

	t.Run("len=0 cap=valid works", func(t *testing.T) {
		buf := make([]byte, 0, 4096)
		bm := NewBitmapToBuf(buf)
		bm.Set(1)
		bm.Set(100)
		require.Equal(t, 2, bm.GetCardinality())
		require.True(t, bm.Contains(1))
		require.True(t, bm.Contains(100))
	})

	t.Run("falls back to heap on too small buffer", func(t *testing.T) {
		bm := NewBitmapToBuf(make([]byte, 10))
		bm.Set(1)
		require.Equal(t, 1, bm.GetCardinality())
		require.True(t, bm.Contains(1))
	})

	t.Run("odd length buffer is truncated", func(t *testing.T) {
		buf := make([]byte, 4097) // odd
		bm := NewBitmapToBuf(buf)
		bm.Set(7)
		require.True(t, bm.Contains(7))
	})

	t.Run("behaves like NewBitmap", func(t *testing.T) {
		buf := make([]byte, 1<<20) // 1MB
		bm1 := NewBitmapToBuf(buf)
		bm2 := NewBitmap()

		for i := uint64(0); i < 5000; i++ {
			bm1.Set(i)
			bm2.Set(i)
		}

		require.Equal(t, bm2.GetCardinality(), bm1.GetCardinality())
		for _, v := range bm2.ToArray() {
			require.True(t, bm1.Contains(v))
		}
	})

	t.Run("no allocation when buffer is large enough for many containers", func(t *testing.T) {
		bufSize := 1 << 20 // 1MB
		bm := NewBitmapToBuf(make([]byte, bufSize))

		require.Equal(t, bufSize, bm.capInBytes())

		// Insert values across many different containers.
		// Each unique high-48-bit key creates a new container.
		// Spread values across 100 containers.
		for container := uint64(0); container < 100; container++ {
			key := container << 16
			for v := uint64(0); v < 100; v++ {
				bm.Set(key | v)
			}
		}

		require.Equal(t, 10000, bm.GetCardinality())
		require.Equal(t, bufSize, bm.capInBytes(), "capacity should not change")
	})

	t.Run("no allocation as keys expand", func(t *testing.T) {
		bufSize := 1 << 20 // 1MB
		bm := NewBitmapToBuf(make([]byte, bufSize))

		require.Equal(t, bufSize, bm.capInBytes())

		// Force many key expansions by creating many distinct containers.
		// Initial key space holds 2 keys; this forces multiple doublings.
		for container := uint64(0); container < 200; container++ {
			bm.Set(container << 16)
		}

		require.Equal(t, 200, bm.GetCardinality())
		require.Equal(t, bufSize, bm.capInBytes(), "capacity should not change")
	})

	t.Run("no allocation with bitmap containers", func(t *testing.T) {
		bufSize := 1 << 20 // 1MB
		bm := NewBitmapToBuf(make([]byte, bufSize))

		require.Equal(t, bufSize, bm.capInBytes())

		// Fill a single container past the array→bitmap conversion threshold
		// (4096+ elements triggers bitmap container, which is 4100 uint16s).
		for v := uint64(0); v < 5000; v++ {
			bm.Set(v)
		}

		require.Equal(t, 5000, bm.GetCardinality())
		require.Equal(t, bufSize, bm.capInBytes(), "capacity should not change")
	})

	t.Run("length grows but capacity stays", func(t *testing.T) {
		bufSize := 1 << 20 // 1MB
		bm := NewBitmapToBuf(make([]byte, bufSize))

		initialLenInBytes := bm.LenInBytes()

		for container := uint64(0); container < 50; container++ {
			bm.Set(container << 16)
		}

		require.Greater(t, bm.LenInBytes(), initialLenInBytes, "length should grow as containers are added")
		require.Equal(t, bufSize, bm.capInBytes(), "capacity should not change")
	})
}

func TestContainer(t *testing.T) {
	ra := NewBitmap()

	// We're creating a container of size 64 words. 4 of these would be used for
	// the header. So, the data can only live in 60 words.
	offset := ra.newContainer(64)
	c := ra.getContainer(offset)
	require.Equal(t, uint16(64), ra.data[offset])
	require.Equal(t, uint16(0), c[indexCardinality])

	fill(c, 0xFF)
	for i, u := range c[startIdx:] {
		if i < 60 {
			require.Equalf(t, uint16(0xFF), u, "at index: %d", i)
		} else {
			require.Equalf(t, uint16(0x00), u, "at index: %d", i)
		}
	}

	offset2 := ra.newContainer(32) // Add a second container.
	c2 := ra.getContainer(offset2)
	require.Equal(t, uint16(32), ra.data[offset2])
	fill(c2, 0xEE)

	// Expand the first container. This would push out the second container, so update its offset.
	ra.expandContainer(offset)
	offset2 += 64

	// Check if the second container is correct.
	c2 = ra.getContainer(offset2)
	require.Equal(t, uint16(32), ra.data[offset2])
	require.Equal(t, 32, len(c2))
	for _, val := range c2[startIdx:] {
		require.Equal(t, uint16(0xEE), val)
	}

	// Check if the first container is correct.
	c = ra.getContainer(offset)
	require.Equal(t, uint16(128), ra.data[offset])
	require.Equal(t, 128, len(c))
	for i, u := range c[startIdx:] {
		if i < 60 {
			require.Equalf(t, uint16(0xFF), u, "at index: %d", i)
		} else {
			require.Equalf(t, uint16(0x00), u, "at index: %d", i)
		}
	}
}

func TestKey(t *testing.T) {
	ra := NewBitmap()
	for i := 1; i <= 10; i++ {
		ra.Set(uint64(i))
	}

	off, has := ra.keys.getValue(0)
	require.True(t, has)
	c := ra.getContainer(off)
	require.Equal(t, uint16(10), c[indexCardinality])

	// Create 10 containers
	for i := 0; i < 10; i++ {
		t.Logf("Creating a new container: %d\n", i)
		ra.Set(uint64(i)<<16 + 1)
	}

	for i := 0; i < 10; i++ {
		ra.Set(uint64(i)<<16 + 2)
	}

	for i := 1; i < 10; i++ {
		offset, has := ra.keys.getValue(uint64(i) << 16)
		require.True(t, has)
		c = ra.getContainer(offset)
		require.Equal(t, uint16(2), c[indexCardinality])
	}

	// Do add in the reverse order.
	for i := 19; i >= 10; i-- {
		ra.Set(uint64(i)<<16 + 2)
	}

	for i := 10; i < 20; i++ {
		offset, has := ra.keys.getValue(uint64(i) << 16)
		require.True(t, has)
		c = ra.getContainer(offset)
		require.Equal(t, uint16(1), c[indexCardinality])
	}
}

func TestEdgeCase(t *testing.T) {
	ra := NewBitmap()

	require.True(t, ra.Set(65536))
	require.True(t, ra.Contains(65536))
}

func TestBulkAdd(t *testing.T) {
	ra := NewBitmap()
	m := make(map[uint64]struct{})
	max := int64(64 << 16)
	start := time.Now()

	var cnt int
	for i := 0; ; i++ {
		if i%100 == 0 && time.Since(start) > time.Second {
			cnt++
			start = time.Now()
			// t.Logf("Bitmap:\n%s\n", ra)
			if cnt == 3 {
				t.Logf("Breaking out of the loop\n")
				break
			}
		}
		x := uint64(rand.Int63n(max))

		if _, has := m[x]; has {
			if !ra.Contains(x) {
				t.Logf("x should be present: %d %#x. Bitmap: %s\n", x, x, ra)
				off, found := ra.keys.getValue(x & mask)
				assert(found)
				c := ra.getContainer(off)
				lo := uint16(x)
				t.Logf("x: %#x lo: %#x. offset: %d\n", x, lo, off)
				switch c[indexType] {
				case typeArray:
				case typeBitmap:
					idx := lo / 16
					pos := lo % 16
					t.Logf("At idx: %d. Pos: %d val: %#b\n", idx, pos, c[startIdx+idx])
				}

				t.Logf("Added: %d %#x. Added: %v\n", x, x, ra.Set(x))
				t.Logf("After add. has: %v\n", ra.Contains(x))

				// 				t.Logf("Hex dump of container at offset: %d\n%s\n", off, hex.Dump(uint16ToByteSliceUnsafe(c)))
				t.FailNow()
			}
			continue
		}
		m[x] = struct{}{}
		// fmt.Printf("Setting x: %#x\n", x)
		if added := ra.Set(x); !added {
			t.Logf("Unable to set: %d %#x\n", x, x)
			t.Logf("ra.Has(x): %v\n", ra.Contains(x))
			t.FailNow()
		}
		// for x := range m {
		// 	if !ra.Has(x) {
		// 		t.Logf("has(x) failed: %#x\n", x)
		// 		t.Logf("Debug: %s\n", ra.Debug(x))
		// 		t.FailNow()
		// 	}
		// }
		// require.Truef(t, ra.Set(x), "Unable to set x: %d %#x\n", x, x)
	}
	t.Logf("Card: %d\n", len(m))
	require.Equalf(t, len(m), ra.GetCardinality(), "Bitmap:\n%s\n", ra)
	for x := range m {
		require.True(t, ra.Contains(x))
	}

	// _, has := ra.keys.getValue(0)
	// require.True(t, has)
	// for i := uint64(1); i <= max; i++ {
	// 	require.Truef(t, ra.Has(i), "i=%d", i)
	// }
	// t.Logf("Data size: %d\n", len(ra.data))

	t.Logf("Copying data. Size: %d\n", len(ra.data))
	dup := make([]uint16, len(ra.data))
	copy(dup, ra.data)

	ra2 := FromBuffer(uint16ToByteSliceUnsafe(dup))
	require.Equal(t, len(m), ra2.GetCardinality())
	for x := range m {
		require.True(t, ra2.Contains(x))
	}
}

func TestBitmapUint64Max(t *testing.T) {
	bm := NewBitmap()

	edges := []uint64{0, math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64}
	for _, e := range edges {
		bm.Set(e)
	}
	for _, e := range edges {
		require.True(t, bm.Contains(e))
	}
}

func TestBitmapZero(t *testing.T) {
	bm1 := NewBitmap()
	bm1.Set(1)
	uids := bm1.ToArray()
	require.Equal(t, 1, len(uids))
	for _, u := range uids {
		require.Equal(t, uint64(1), u)
	}

	bm2 := NewBitmap()
	bm2.Set(2)

	bm3 := Or(bm1, bm2)
	require.False(t, bm3.Contains(0))
	require.True(t, bm3.Contains(1))
	require.True(t, bm3.Contains(2))
	require.Equal(t, 2, bm3.GetCardinality())
}

func TestBitmapOps(t *testing.T) {
	M := int64(10000)
	// smaller bitmap would always operate with [0, M) range.
	// max for each bitmap = M * F
	F := []int64{1, 10, 100, 1000}
	N := 10000

	for _, f := range F {
		t.Logf("Using N: %d M: %d F: %d\n", N, M, f)
		small, big := NewBitmap(), NewBitmap()
		occ := make(map[uint64]int)
		smallMap := make(map[uint64]struct{})
		bigMap := make(map[uint64]struct{})

		for i := 0; i < N; i++ {
			smallx := uint64(rand.Int63n(M))

			_, has := smallMap[smallx]
			added := small.Set(smallx)
			if has {
				require.False(t, added, "Can't readd already present x: %d", smallx)
			}
			smallMap[smallx] = struct{}{}

			bigx := uint64(rand.Int63n(M * f))
			_, has = bigMap[bigx]
			added = big.Set(bigx)
			if has {
				require.False(t, added, "Can't readd already present x: %d", bigx)
			}
			bigMap[bigx] = struct{}{}

			occ[smallx] |= 0x01 // binary 0001
			occ[bigx] |= 0x02   // binary 0010
		}
		require.Equal(t, len(smallMap), small.GetCardinality())
		require.Equal(t, len(bigMap), big.GetCardinality())

		bitOr := Or(small, big)
		bitAnd := And(small, big)

		t.Logf("Sizes. small: %d big: %d, bitOr: %d bitAnd: %d\n",
			small.GetCardinality(), big.GetCardinality(),
			bitOr.GetCardinality(), bitAnd.GetCardinality())

		cntOr, cntAnd := 0, 0
		for x, freq := range occ {
			if freq == 0x00 {
				require.Failf(t, "Failed", "Value of freq can't be zero. Found: %#x\n", freq)
			} else if freq == 0x01 {
				_, has := smallMap[x]
				require.True(t, has)
				require.True(t, small.Contains(x))
				require.Truef(t, bitOr.Contains(x), "Expected %d %#x. But, not found. freq: %#x\n",
					x, x, freq)
				cntOr++

			} else if freq == 0x02 {
				// one of them has it.
				_, has := bigMap[x]
				require.True(t, has)
				require.True(t, big.Contains(x))
				require.Truef(t, bitOr.Contains(x), "Expected %d %#x. But, not found. freq: %#x\n",
					x, x, freq)
				cntOr++

			} else if freq == 0x03 {
				require.True(t, small.Contains(x))
				require.True(t, big.Contains(x))
				require.Truef(t, bitAnd.Contains(x), "x: %#x\n", x)
				cntOr++
				cntAnd++
			} else {
				require.Failf(t, "Failed", "Value of freq can't exceed 0x03. Found: %#x\n", freq)
			}
		}
		if cntAnd != bitAnd.GetCardinality() {
			uids := bitAnd.ToArray()
			t.Logf("Len Uids: %d Card: %d cntAnd: %d. Occ: %d\n", len(uids), bitAnd.GetCardinality(), cntAnd, len(occ))

			uidMap := make(map[uint64]struct{})
			for _, u := range uids {
				uidMap[u] = struct{}{}
			}
			for u := range occ {
				delete(uidMap, u)
			}
			for x := range uidMap {
				t.Logf("Remaining uids in UidMap: %d %#b\n", x, x)
			}
			require.FailNow(t, "Cardinality isn't matching")
		}
		require.Equal(t, cntOr, bitOr.GetCardinality())
		require.Equal(t, cntAnd, bitAnd.GetCardinality())
	}
}

func TestUint16(t *testing.T) {
	a := uint16(0xfeff)
	b := uint16(0x100)
	t.Logf("a & b: %#x", a&b)
	var x uint16
	for i := 0; i < 100000; i++ {
		prev := x
		x++
		if x <= prev {
			// This triggers when prev = 0xFFFF.
			// require.Failf(t, "x<=prev", "x %d <= prev %d", x, prev)
		}
	}
}

func TestSetGet(t *testing.T) {
	bm := NewBitmap()
	N := int(1e6)
	for i := 0; i < N; i++ {
		bm.Set(uint64(i))
	}
	for i := 0; i < N; i++ {
		has := bm.Contains(uint64(i))
		require.True(t, has)
	}
}

func TestSetSorted(t *testing.T) {
	check := func(n int) {
		var arr []uint64
		for i := 0; i < n; i++ {
			arr = append(arr, uint64(i))
		}
		r := FromSortedList(arr)
		require.Equal(t, len(arr), r.GetCardinality())

		rarr := r.ToArray()
		for i := 0; i < n; i++ {
			require.Equal(t, uint64(i), rarr[i])
		}

		r.Set(uint64(n))
		require.True(t, r.Contains(uint64(n)))
	}
	check(10)
	check(1e6)
}

func TestFromSortedListContainerAlignment(t *testing.T) {
	// Container payloads are read through uint64 views
	// (uint16To64SliceUnsafe), so every container must start 8-byte
	// aligned. containerSizeForCard keeps sizes at multiples of 4 uint16s
	// even for small array containers, so no container can misalign the
	// ones that follow it.
	bm := FromSortedList([]uint64{1, 2, 3, 1 << 20, 1<<20 + 1, 1 << 40})
	n := bm.keys.numKeys()
	require.Equal(t, 3, n)
	for i := 0; i < n; i++ {
		require.Zero(t, bm.keys.val(i)%4)
	}
}

func TestSetIntoCompactedArrayContainer(t *testing.T) {
	// containerSizeForCard sizes an array to its cardinality alone, so a
	// fresh container can arrive full (2044, the largest array that fits
	// maxArrayContainerSize); growing past it and across the array-bitmap
	// cutoff must work for containers built by both exact-size constructors.
	for _, n := range []int{2043, 2044, 2045} {
		vals := make([]uint64, n)
		for i := range vals {
			vals[i] = uint64(i) * 2 // even values: odd ones stay free to Set
		}
		builders := map[string]func() *Bitmap{
			"FromSortedList": func() *Bitmap { return FromSortedList(vals) },
			"Accumulator": func() *Bitmap {
				acc := NewAccumulator()
				acc.Or(bitmapOf(vals...))
				return acc.Bitmap()
			},
		}
		for name, build := range builders {
			t.Run(fmt.Sprintf("%s n=%d", name, n), func(t *testing.T) {
				bm := build()
				for j := 0; j < 5; j++ {
					v := uint64(j)*2 + 1
					require.True(t, bm.Set(v))
					require.True(t, bm.Contains(v))
				}
				require.Equal(t, n+5, bm.GetCardinality())
			})
		}
	}
}

// testMaskedCommon runs the shared correctness cases for Masked and MaskedToBuf.
// masker abstracts the call so each test function can pass its own implementation.
func testMaskedCommon(t *testing.T, masker func(bm *Bitmap, mask uint64) *Bitmap) {
	t.Helper()

	t.Run("no bitmap", func(t *testing.T) {
		var bm *Bitmap
		require.Equal(t, 0, masker(bm, 0x0000FFFFFFFFFFFF).GetCardinality())
	})

	t.Run("empty bitmap", func(t *testing.T) {
		require.Equal(t, 0, masker(NewBitmap(), 0x0000FFFFFFFFFFFF).GetCardinality())
	})

	t.Run("all bits mask is identity", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(0x00010000) // key=0x10000, container value=0
		bm.Set(0x00020000) // key=0x20000, container value=0
		bm.Set(0x00010001) // key=0x10000, container value=1

		result := masker(bm, math.MaxUint64)

		require.Equal(t, 3, result.GetCardinality())
		require.True(t, result.Contains(0x00010000))
		require.True(t, result.Contains(0x00020000))
		require.True(t, result.Contains(0x00010001))
	})

	t.Run("zero mask collapses all keys", func(t *testing.T) {
		bm := NewBitmap()
		// Values that differ only in the key portion (bits 16+) collapse,
		// but their low 16 bits (container values) are preserved and merged.
		bm.Set(0x00010000) // key=0x10000, container value=0
		bm.Set(0x00020000) // key=0x20000, container value=0
		bm.Set(0x00010001) // key=0x10000, container value=1

		result := masker(bm, 0)

		// All keys become 0, containers merge: values 0 and 1
		require.Equal(t, 2, result.GetCardinality())
		require.True(t, result.Contains(0))
		require.True(t, result.Contains(1))
	})

	t.Run("mask preserving middle bits", func(t *testing.T) {
		// Use a mask that keeps bits 16-31 and zeroes bits 32-63.
		// This groups values that share the same bits 16-31.
		var m uint64 = 0x00000000FFFF0000

		bm := NewBitmap()
		// Two values with same bits 16-31 but different bits 32+
		bm.Set(0x0001_0005_0003) // key bits 16-31 = 0x0005, bits 32+ = 0x0001
		bm.Set(0x0002_0005_0007) // key bits 16-31 = 0x0005, bits 32+ = 0x0002
		bm.Set(0x0003_0005_0007) // key bits 16-31 = 0x0005, bits 32+ = 0x0002

		result := masker(bm, m)

		// Both collapse to masked key 0x00050000, container values 3 and 7 merged
		require.Equal(t, 2, result.GetCardinality())
		require.True(t, result.Contains(0x00050003))
		require.True(t, result.Contains(0x00050007))
	})

	t.Run("does not modify original", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(uint64(1)<<48 | 10)
		bm.Set(uint64(2)<<48 | 20)

		origCard := bm.GetCardinality()
		_ = masker(bm, 0x0000FFFFFFFFFFFF)

		require.Equal(t, origCard, bm.GetCardinality())
		require.True(t, bm.Contains(uint64(1)<<48|10))
		require.True(t, bm.Contains(uint64(2)<<48|20))
	})

	t.Run("many keys collapsing", func(t *testing.T) {
		bm := NewBitmap()
		numPositions := uint16(10)
		numValues := uint64(1000)

		for pos := uint16(0); pos < numPositions; pos++ {
			for v := uint64(0); v < numValues; v++ {
				bm.Set(uint64(pos)<<48 | v)
			}
		}

		result := masker(bm, 0x0000FFFFFFFFFFFF)

		require.Equal(t, int(numValues), result.GetCardinality())
		for v := uint64(0); v < numValues; v++ {
			require.True(t, result.Contains(v))
		}
	})

	t.Run("non-contiguous masked keys merge correctly", func(t *testing.T) {
		// Source keys are sorted by full value. When the mask zeroes high
		// bits, keys that map to the same masked key may NOT be adjacent
		// in source order. All such keys must still be OR-merged.
		bm := NewBitmap()

		// Two positions (high bits differ), two groups (middle bits differ).
		// Each (pos,group) has distinct values so we can detect lost merges.
		// pos=0 group=0: values {0, 1}
		bm.Set(0x0001_0000_0000 | 0)
		bm.Set(0x0001_0000_0000 | 1)
		// pos=0 group=1: values {10, 11}
		bm.Set(0x0001_0001_0000 | 10)
		bm.Set(0x0001_0001_0000 | 11)
		// pos=1 group=0: values {2, 3}  — same masked key as pos=0 group=0
		bm.Set(0x0002_0000_0000 | 2)
		bm.Set(0x0002_0000_0000 | 3)
		// pos=1 group=1: values {12, 13} — same masked key as pos=0 group=1
		bm.Set(0x0002_0001_0000 | 12)
		bm.Set(0x0002_0001_0000 | 13)

		// Mask zeroes bits 32-63, keeps bits 16-31.
		// group=0 keys collapse to masked key 0x00000000
		// group=1 keys collapse to masked key 0x00010000
		result := masker(bm, 0x00000000FFFF0000)

		// group=0 must contain OR of {0,1} and {2,3}
		require.True(t, result.Contains(0), "group 0 missing value 0")
		require.True(t, result.Contains(1), "group 0 missing value 1")
		require.True(t, result.Contains(2), "group 0 missing value 2 (from pos=1)")
		require.True(t, result.Contains(3), "group 0 missing value 3 (from pos=1)")

		// group=1 must contain OR of {10,11} and {12,13}
		require.True(t, result.Contains(0x0001_0000|10), "group 1 missing value 10")
		require.True(t, result.Contains(0x0001_0000|11), "group 1 missing value 11")
		require.True(t, result.Contains(0x0001_0000|12), "group 1 missing value 12 (from pos=1)")
		require.True(t, result.Contains(0x0001_0000|13), "group 1 missing value 13 (from pos=1)")

		require.Equal(t, 8, result.GetCardinality())
	})

	t.Run("low 16 bits of mask are ignored", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(uint64(1)<<48 | 1)
		bm.Set(uint64(2)<<48 | 1)

		// Passing mask with low bits set should behave the same
		// because keys always have low 16 bits unset.
		r1 := masker(bm, 0x0000FFFFFFFFFFFF)
		r2 := masker(bm, 0x0000FFFFFFFFFFFF|0xFFFF)

		require.Equal(t, r1.GetCardinality(), r2.GetCardinality())
		for _, v := range r1.ToArray() {
			require.True(t, r2.Contains(v))
		}
	})

	t.Run("trim-and-regrow: no dead space when inline OR fails on last container", func(t *testing.T) {
		// Two source keys collapse to the same masked key with non-overlapping
		// values. The first source container (50 elements, 54 uint16s) is copied
		// into the result. When the second source key is processed the OR result
		// (100 elements, 104 uint16s) does not fit inline in the 54-uint16 slot.
		// Since this is the only container in the result it is always the last
		// one, so the trim-and-regrow path fires: result.data is truncated back
		// to before the old container before the new one is appended.
		//
		// Without the optimisation the old 54-uint16 container would remain as
		// dead space, making LenInBytes 108 bytes larger than necessary.
		bm := NewBitmap()
		for v := uint64(0); v < 50; v++ {
			bm.Set(0x0001_0000_0000 | v) // key 1 → masked 0, values 0-49
		}
		for v := uint64(50); v < 100; v++ {
			bm.Set(0x0002_0000_0000 | v) // key 2 → masked 0, values 50-99 (non-overlapping)
		}

		result := masker(bm, 0x0000_0000_FFFF_0000)

		require.Equal(t, 100, result.GetCardinality())
		for v := uint64(0); v < 100; v++ {
			require.True(t, result.Contains(v), "missing value %d", v)
		}

		// Verify no dead space: total data must equal keys section + single container.
		// If dead space existed it would add 108 bytes (the old 54-uint16 container).
		containerOffset := result.keys.val(0)
		containerSize := int(result.data[containerOffset]) // first uint16 of container = its size
		expectedLen := (result.keys.size() + containerSize) * 2
		require.Equal(t, expectedLen, result.LenInBytes(),
			"LenInBytes should equal keys section + container size with no dead space")
	})

	t.Run("trim-and-regrow: no dead space when array-to-bitmap conversion on last container", func(t *testing.T) {
		// Two source keys collapse to the same masked key. Each has 1228
		// non-overlapping values. When OR'd, cnum+onum = 2456 which meets the
		// array-to-bitmap conversion threshold (maxContainerSize/5*3 - startIdx).
		// The first source container (1228 elements, 1232 uint16s) is copied into
		// the result as an array. The OR with the second source container triggers
		// conversion to a bitmap (4100 uint16s) which cannot fit inline in the
		// 1232-uint16 slot. Since this is the only (last) container in the result,
		// the trim-and-regrow path fires, leaving no dead space.
		//
		// Without the optimisation the old 1232-uint16 array container would
		// remain as dead space, making LenInBytes 2464 bytes larger than necessary.
		const half = 1228 // cnum+onum = 2456 = maxContainerSize/5*3 - startIdx
		bm := NewBitmap()
		for v := uint64(0); v < half; v++ {
			bm.Set(0x0001_0000_0000 | v) // key 1 → masked 0, values 0-1227
		}
		for v := uint64(half); v < 2*half; v++ {
			bm.Set(0x0002_0000_0000 | v) // key 2 → masked 0, values 1228-2455 (non-overlapping)
		}

		result := masker(bm, 0x0000_0000_FFFF_0000)

		require.Equal(t, 2*half, result.GetCardinality())
		for v := uint64(0); v < 2*half; v++ {
			require.True(t, result.Contains(v), "missing value %d", v)
		}

		// Verify no dead space: total data must equal keys section + bitmap container.
		// If dead space existed it would add 2464 bytes (the old 1232-uint16 array).
		containerOffset := result.keys.val(0)
		containerSize := int(result.data[containerOffset]) // first uint16 = size = 4100 for bitmap
		expectedLen := (result.keys.size() + containerSize) * 2
		require.Equal(t, expectedLen, result.LenInBytes(),
			"LenInBytes should equal keys section + bitmap container size with no dead space")
	})
}

func TestMasked(t *testing.T) {
	testMaskedCommon(t, func(bm *Bitmap, mask uint64) *Bitmap {
		return bm.Masked(mask)
	})
}

func TestMaskedToBuf(t *testing.T) {
	testMaskedCommon(t, func(bm *Bitmap, mask uint64) *Bitmap {
		return bm.MaskedToBuf(mask, make([]byte, 1<<20))
	})

	t.Run("matches Masked results", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(uint64(1)<<48 | 1)
		bm.Set(uint64(2)<<48 | 1)
		bm.Set(uint64(3)<<48 | 2)
		bm.Set(0x0001_0005_0003)
		bm.Set(0x0002_0005_0007)

		masks := []uint64{0, 0x0000FFFFFFFFFFFF, math.MaxUint64, 0x00000000FFFF0000}
		for _, m := range masks {
			expected := bm.Masked(m)
			got := bm.MaskedToBuf(m, make([]byte, 1<<20))

			require.Equal(t, expected.GetCardinality(), got.GetCardinality())
			for _, v := range expected.ToArray() {
				require.True(t, got.Contains(v))
			}
		}
	})

	t.Run("no allocation when buffer is large enough", func(t *testing.T) {
		bm := NewBitmap()
		numPositions := uint16(10)
		numValues := uint64(1000)

		for pos := uint16(0); pos < numPositions; pos++ {
			for v := uint64(0); v < numValues; v++ {
				bm.Set(uint64(pos)<<48 | v)
			}
		}

		bufSize := 1 << 20 // 1MB
		result := bm.MaskedToBuf(0x0000FFFFFFFFFFFF, make([]byte, bufSize))

		require.Equal(t, int(numValues), result.GetCardinality())
		require.Equal(t, bufSize, result.capInBytes(), "capacity should not change")
	})

	t.Run("length grows but capacity stays", func(t *testing.T) {
		bm := NewBitmap()
		for container := uint64(0); container < 50; container++ {
			bm.Set(container << 16)
		}

		bufSize := 1 << 20 // 1MB
		result := bm.MaskedToBuf(math.MaxUint64, make([]byte, bufSize))

		require.Equal(t, 50, result.GetCardinality())
		require.Greater(t, result.LenInBytes(), 0)
		require.Equal(t, bufSize, result.capInBytes(), "capacity should not change")
	})
}

func TestAnd(t *testing.T) {
	a := NewBitmap()
	b := NewBitmap()

	N := int(1e7)
	for i := 0; i < N; i++ {
		if i%2 == 0 {
			a.Set(uint64(i))
		} else {
			b.Set(uint64(i))
		}
	}
	require.Equal(t, N/2, a.GetCardinality())
	require.Equal(t, N/2, b.GetCardinality())
	res := And(a, b)
	require.Equal(t, 0, res.GetCardinality())
	a.And(b)
	require.Equal(t, 0, a.GetCardinality())
}

func TestAnd2(t *testing.T) {
	a := NewBitmap()
	n := int(1e7)

	for i := 0; i < n; i++ {
		a.Set(uint64(i))
	}
	require.Equal(t, n, a.GetCardinality())
	a.RemoveRange(0, uint64(n/2))

	for i := 0; i < n; i++ {
		a.Set(uint64(i))
	}
	require.Equal(t, n, a.GetCardinality())
}

func TestAndNot(t *testing.T) {
	a := NewBitmap()
	b := NewBitmap()

	N := int(1e7)
	for i := 0; i < N; i++ {
		a.Set(uint64(i))
		if i < N/2 {
			b.Set(uint64(i))
		}
	}
	require.Equal(t, N, a.GetCardinality())
	require.Equal(t, N/2, b.GetCardinality())

	a.AndNot(b)
	require.Equal(t, N/2, a.GetCardinality())

	// Test for case when array container will be generated.
	a = NewBitmap()
	b = NewBitmap()

	a.SetMany([]uint64{1, 2, 3, 4})
	b.SetMany([]uint64{3, 4, 5, 6})

	a.AndNot(b)
	require.Equal(t, []uint64{1, 2}, a.ToArray())

	// Test for case when bitmap container will be generated.
	a = NewBitmap()
	b = NewBitmap()
	for i := 0; i < 10000; i++ {
		a.Set(uint64(i))
		if i < 7000 {
			b.Set(uint64(i))
		}
	}
	a.AndNot(b)
	require.Equal(t, 3000, a.GetCardinality())
	for i := 0; i < 10000; i++ {
		if i < 7000 {
			require.False(t, a.Contains(uint64(i)))
		} else {
			require.True(t, a.Contains(uint64(i)))
		}
	}
}

func TestAndNot2(t *testing.T) {
	a := NewBitmap()
	b := NewBitmap()
	n := int(1e6)

	for i := 0; i < n/2; i++ {
		a.Set(uint64(i))
	}
	for i := n / 2; i < n; i++ {
		b.Set(uint64(i))
	}
	require.Equal(t, n/2, a.GetCardinality())
	a.AndNot(b)
	require.Equal(t, n/2, a.GetCardinality())

}

func TestOr(t *testing.T) {
	a := NewBitmap()
	b := NewBitmap()

	N := int(1e7)
	for i := 0; i < N; i++ {
		if i%2 == 0 {
			a.Set(uint64(i))
		} else {
			b.Set(uint64(i))
		}
	}
	require.Equal(t, N/2, a.GetCardinality())
	require.Equal(t, N/2, b.GetCardinality())
	res := Or(a, b)
	require.Equal(t, N, res.GetCardinality())
	a.or(b, 0)
	require.Equal(t, N, a.GetCardinality())
}

func TestCardinality(t *testing.T) {
	a := NewBitmap()
	n := 1 << 20
	for i := 0; i < n; i++ {
		a.Set(uint64(i))
	}
	require.Equal(t, n, a.GetCardinality())
}

func TestRemove(t *testing.T) {
	a := NewBitmap()
	N := int(1e7)
	for i := 0; i < N; i++ {
		a.Set(uint64(i))
	}
	require.Equal(t, N, a.GetCardinality())
	for i := 0; i < N/2; i++ {
		require.True(t, a.Remove(uint64(i)))
	}
	require.Equal(t, N/2, a.GetCardinality())

	// Remove elelemts which doesn't exist should be no-op
	for i := 0; i < N/2; i++ {
		require.False(t, a.Remove(uint64(i)))
	}
	require.Equal(t, N/2, a.GetCardinality())

	for i := 0; i < N/2; i++ {
		require.True(t, a.Remove(uint64(i+N/2)))
	}
	require.Equal(t, 0, a.GetCardinality())
}

func TestContainerRemoveRange(t *testing.T) {
	ra := NewBitmap()

	type cases struct {
		lo       uint16
		hi       uint16
		expected []uint16
	}

	testBitmap := func(tc cases) {
		offset := ra.newContainer(maxContainerSize)
		c := ra.getContainer(offset)
		c[indexType] = typeBitmap
		a := bitmap(c)

		for i := 1; i <= 5; i++ {
			a.add(uint16(5 * i))
		}
		a.removeRange(tc.lo, tc.hi)
		result := a.all()
		require.Equalf(t, len(tc.expected), getCardinality(a), "case: %+v, actual:%v\n", tc, result)
		require.Equalf(t, tc.expected, result, "case: %+v actual: %v\n", tc, result)
	}

	testArray := func(tc cases) {
		offset := ra.newContainer(maxContainerSize)
		c := ra.getContainer(offset)
		c[indexType] = typeArray
		a := array(c)

		for i := 1; i <= 5; i++ {
			a.add(uint16(5 * i))
		}
		a.removeRange(tc.lo, tc.hi)
		result := a.all()
		require.Equalf(t, len(tc.expected), getCardinality(a), "case: %+v, actual:%v\n", tc, result)
		require.Equalf(t, tc.expected, result, "case: %+v actual: %v\n", tc, result)
	}

	tests := []cases{
		{8, 22, []uint16{5, 25}},
		{8, 20, []uint16{5, 25}},
		{10, 22, []uint16{5, 25}},
		{10, 20, []uint16{5, 25}},
		{7, 11, []uint16{5, 15, 20, 25}},
		{7, 10, []uint16{5, 15, 20, 25}},
		{10, 11, []uint16{5, 15, 20, 25}},
		{0, 0, []uint16{5, 10, 15, 20, 25}},
		{30, 30, []uint16{5, 10, 15, 20, 25}},
	}

	for _, tc := range tests {
		testBitmap(tc)
		testArray(tc)
	}
}

func TestRemoveRange(t *testing.T) {
	a := NewBitmap()
	N := int(1e7)
	for i := 0; i < N; i++ {
		a.Set(uint64(i))
	}
	a.RemoveRange(0, 0)
	require.Equal(t, N, a.GetCardinality())

	require.Equal(t, N, a.GetCardinality())
	a.RemoveRange(uint64(N/4), uint64(N/2))
	require.Equal(t, 3*N/4, a.GetCardinality())

	a.RemoveRange(0, uint64(N/2))
	require.Equal(t, N/2, a.GetCardinality())

	a.RemoveRange(uint64(N/2), uint64(N))
	require.Equal(t, 0, a.GetCardinality())
	a.Set(uint64(N / 4))
	a.Set(uint64(N / 2))
	a.Set(uint64(3 * N / 4))
	require.Equal(t, 3, a.GetCardinality())

	var arr []uint64
	for i := 0; i < 123; i++ {
		arr = append(arr, uint64(i))
	}
	b := FromSortedList(arr)
	b.RemoveRange(50, math.MaxUint64)
	require.Equal(t, 50, b.GetCardinality())
}

func TestRemoveRange2(t *testing.T) {
	// High from the last container should not be removed.
	a := NewBitmap()
	for i := 1; i < 10; i++ {
		a.Set(uint64(i * (1 << 16)))
		a.Set(uint64(i*(1<<16)) - 1)
	}
	a.RemoveRange(1<<16, (4<<16)-1)
	require.True(t, a.Contains((4<<16)-1))
}

func TestSelect(t *testing.T) {
	a := NewBitmap()
	N := int(1e4)
	for i := 0; i < N; i++ {
		a.Set(uint64(i))
	}
	for i := 0; i < N; i++ {
		val, err := a.Select(uint64(i))
		require.NoError(t, err)
		require.Equal(t, uint64(i), val)
	}
}

func TestClone(t *testing.T) {
	a := NewBitmap()
	N := int(1e5)

	for i := 0; i < N; i++ {
		a.Set(uint64(rand.Int63n(math.MaxInt64)))
	}
	b := a.Clone()
	require.Equal(t, a.GetCardinality(), b.GetCardinality())
	require.Equal(t, a.ToArray(), b.ToArray())
}

func TestContainerFull(t *testing.T) {
	c := make([]uint16, maxContainerSize)
	b := bitmap(c)
	b[indexType] = typeBitmap
	b[indexSize] = maxContainerSize
	for i := 0; i < 1<<16; i++ {
		b.add(uint16(i))
	}
	require.Equal(t, math.MaxUint16+1, getCardinality(b))

	c2 := make([]uint16, maxContainerSize)
	copy(c2, c)
	b2 := bitmap(c2)

	b.orBitmap(b2, nil, runInline)
	require.Equal(t, math.MaxUint16+1, getCardinality(b))

	setCardinality(b, invalidCardinality)
	b.orBitmap(b2, nil, runInline)
	require.Equal(t, invalidCardinality, getCardinality(b))

	setCardinality(b, b.cardinality())
	require.Equal(t, maxCardinality, getCardinality(b))
}

func TestExtremes(t *testing.T) {
	a := NewBitmap()
	require.Equal(t, uint64(0), a.Minimum())
	require.Equal(t, uint64(0), a.Maximum())

	a.Set(1)
	require.Equal(t, uint64(1), a.Minimum())
	require.Equal(t, uint64(1), a.Maximum())

	a.Set(100000)
	require.Equal(t, uint64(1), a.Minimum())
	require.Equal(t, uint64(100000), a.Maximum())

	a.Remove(100000)
	require.Equal(t, uint64(1), a.Minimum())
	require.Equal(t, uint64(1), a.Maximum())

	a.Remove(1)
	require.Equal(t, uint64(0), a.Minimum())
	require.Equal(t, uint64(0), a.Maximum())

	a.Set(100000)
	require.Equal(t, uint64(100000), a.Minimum())
	require.Equal(t, uint64(100000), a.Maximum())

	a.Remove(100000)
	for i := 0; i <= maxContainerSize; i++ {
		a.Set(uint64(i))
	}
	require.Equal(t, uint64(0), a.Minimum())
	require.Equal(t, uint64(maxContainerSize), a.Maximum())
}

func TestCleanup(t *testing.T) {
	a := NewBitmap()
	n := 10

	for i := 0; i < n; i++ {
		a.Set(uint64((i * (1 << 16))))
	}
	abuf := a.ToBufferWithCopy()

	require.Equal(t, 10, a.keys.numKeys())
	a.RemoveRange(1<<16, 2*(1<<16))
	require.Equal(t, 9, a.keys.numKeys())

	a.RemoveRange(6*(1<<16), 8*(1<<16))
	require.Equal(t, 7, a.keys.numKeys())

	a = FromBufferWithCopy(abuf)
	require.Equal(t, 10, a.keys.numKeys())
	a.Remove(6 * (1 << 16))
	a.RemoveRange(7*(1<<16), 9*(1<<16))
	require.Equal(t, 7, a.keys.numKeys())

	n = int(1e6)
	b := NewBitmap()
	for i := 0; i < n; i++ {
		b.Set(uint64(i))
	}
	b.RemoveRange(0, uint64(n/2))
	require.Equal(t, n/2, b.GetCardinality())
	buf := b.ToBuffer()
	b = FromBuffer(buf)
	require.Equal(t, n/2, b.GetCardinality())
}

func TestCleanup2(t *testing.T) {
	a := NewBitmap()
	n := 10
	for i := 0; i < n; i++ {
		a.Set(uint64(i * (1 << 16)))
	}
	require.Equal(t, n, a.GetCardinality())
	require.Equal(t, n, a.keys.numKeys())

	for i := 0; i < n; i++ {
		if i%2 == 1 {
			a.Remove(uint64(i * (1 << 16)))
		}
	}
	require.Equal(t, n/2, a.GetCardinality())
	require.Equal(t, n, a.keys.numKeys())

	a.Cleanup()
	require.Equal(t, n/2, a.GetCardinality())
	require.Equal(t, n/2, a.keys.numKeys())
}

func TestCleanupSplit(t *testing.T) {
	a := NewBitmap()
	n := int(1e8)

	for i := 0; i < n; i++ {
		a.Set(uint64(i))
	}

	split := func() {
		n := a.GetCardinality()
		mid, err := a.Select(uint64(n / 2))
		require.NoError(t, err)

		b := a.Clone()
		a.RemoveRange(0, mid)
		b.RemoveRange(mid, math.MaxUint64)

		require.Equal(t, n, a.GetCardinality()+b.GetCardinality())
	}
	for a.GetCardinality() > 1 {
		split()
	}
}

func TestIsEmpty(t *testing.T) {
	a := NewBitmap()
	require.True(t, a.IsEmpty())

	n := int(1e6)
	for i := 0; i < n; i++ {
		a.Set(uint64(i))
	}
	require.False(t, a.IsEmpty())
	a.RemoveRange(0, math.MaxUint64)
	require.True(t, a.IsEmpty())
}

func TestRank(t *testing.T) {
	a := NewBitmap()
	n := int(1e6)
	for i := uint64(0); i < uint64(n); i++ {
		a.Set(i)
	}
	for i := 0; i < n; i++ {
		require.Equal(t, i, a.Rank(uint64(i)))
	}
	require.Equal(t, -1, a.Rank(uint64(n)))

	// Check ranks after removing an element.
	a.Remove(100)
	for i := 0; i < n; i++ {
		if i < 100 {
			require.Equal(t, i, a.Rank(uint64(i)))
		} else if i == 100 {
			require.Equal(t, -1, a.Rank(uint64(i)))
		} else {
			require.Equal(t, i-1, a.Rank(uint64(i)))
		}
	}

	// Check ranks after removing a range of elements.
	a.RemoveRange(0, uint64(1e4))
	for i := 0; i < n; i++ {
		if i < 1e4 {
			require.Equal(t, -1, a.Rank(uint64(n)))
		} else {
			require.Equal(t, i-1e4, a.Rank(uint64(i)))
		}
	}
}

func TestSplit(t *testing.T) {
	run := func(n int) {
		r := NewBitmap()
		for i := 1; i <= n; i++ {
			r.Set(uint64(i))
		}
		f := func(start, end uint64) uint64 { return 0 }

		// Split the bitmaps.
		bms := r.Split(f, 1<<10)
		var csum int
		for _, bm := range bms {
			csum += bm.GetCardinality()
		}
		require.Equal(t, n, csum)

		id := uint64(1)
		for _, bm := range bms {
			itr := bm.NewIterator()
			for cur := itr.Next(); cur != 0; cur = itr.Next() {
				require.Equal(t, id, cur)
				id++
			}
		}
	}

	run(2)
	run(11)
	run(1e3)
	run(1e6)
}

// Test making sure out of range panic does not occur anymore
// https://github.com/weaviate/sroar/issues/1
//
// panic: runtime error: slice bounds out of range [:204] with capacity 64
func Test_Issue_1(t *testing.T) {

	genBitmap := func(fromInc, toExc uint64) *Bitmap {
		bm := NewBitmap()
		for i := fromInc; i < toExc; i++ {
			bm.Set(i)
		}
		return bm
	}

	genSlice := func(fromInc, toExc uint64) []uint64 {
		slice := []uint64{}
		for i := fromInc; i < toExc; i++ {
			slice = append(slice, i)
		}
		return slice
	}

	type testCase struct {
		name             string
		leftBm           *Bitmap
		rightBm          *Bitmap
		nextLeftBm       *Bitmap
		expectedElements []uint64
	}

	testCases := []testCase{
		{
			// returns 30x 65536 instead of 71970..71999
			name:       "array of 60 elements in left bitmap, 2 internal containers (numbers > 2^16), empty bitmap on left Or",
			leftBm:     genBitmap(71_970, 72_030),
			rightBm:    genBitmap(72_000, 75_000),
			nextLeftBm: NewBitmap(),

			expectedElements: genSlice(71_970, 72_000),
		},
		{
			// working (sanity check)
			name:       "array of 60 elements in left bitmap, 1 internal container (numbers < 2^16), empty bitmap on left Or",
			leftBm:     genBitmap(970, 1_030),
			rightBm:    genBitmap(1_000, 4_000),
			nextLeftBm: NewBitmap(),

			expectedElements: genSlice(970, 1_000),
		},
		{
			// panic: runtime error: slice bounds out of range [:204] with capacity 64
			name:       "array of 300 elements in left bitmap, 2 internal containers (numbers > 2^16), empty bitmap on left Or",
			leftBm:     genBitmap(71_800, 72_100),
			rightBm:    genBitmap(72_000, 75_000),
			nextLeftBm: NewBitmap(),

			expectedElements: genSlice(71_800, 72_000),
		},
		{
			// working (sanity check)
			name:       "array of 300 elements in left bitmap, 1 internal container (numbers < 2^16), empty bitmap on left Or",
			leftBm:     genBitmap(800, 1_100),
			rightBm:    genBitmap(1_000, 4_000),
			nextLeftBm: NewBitmap(),

			expectedElements: genSlice(800, 1_000),
		},
		{
			// working (sanity check)
			name:       "array of 60 elements in left bitmap, 2 internal containers (numbers > 2^16), non-empty bitmap on left Or",
			leftBm:     genBitmap(71_970, 72_030),
			rightBm:    genBitmap(72_000, 75_000),
			nextLeftBm: genBitmap(71_980, 72_000),

			expectedElements: genSlice(71_970, 72_000),
		},
		{
			// working (sanity check)
			name:       "array of 60 elements in left bitmap, 1 internal container (numbers < 2^16), non-empty bitmap on left Or",
			leftBm:     genBitmap(970, 1_030),
			rightBm:    genBitmap(1_000, 4_000),
			nextLeftBm: genBitmap(980, 1_000),

			expectedElements: genSlice(970, 1_000),
		},
		{
			// working (sanity check)
			name:       "array of 300 elements in left bitmap, 2 internal containers (numbers > 2^16), non-empty bitmap on left Or",
			leftBm:     genBitmap(71_800, 72_100),
			rightBm:    genBitmap(72_000, 75_000),
			nextLeftBm: genBitmap(71_900, 72_000),

			expectedElements: genSlice(71_800, 72_000),
		},
		{
			// working (sanity check)
			name:       "array of 300 elements in left bitmap, 1 internal container (numbers < 2^16), non-empty bitmap on left Or",
			leftBm:     genBitmap(800, 1_100),
			rightBm:    genBitmap(1_000, 4_000),
			nextLeftBm: genBitmap(900, 1_000),

			expectedElements: genSlice(800, 1_000),
		},
		{
			// returns 30x 65536 instead of 71970..71999
			name:       "array of 60 elements in left bitmap, 2 internal containers (numbers > 2^16), non-empty (other container) bitmap on left Or",
			leftBm:     genBitmap(71_970, 72_030),
			rightBm:    genBitmap(72_000, 75_000),
			nextLeftBm: genBitmap(970, 1_000),

			expectedElements: append(genSlice(970, 1_000), genSlice(71_970, 72_000)...),
		},
		{
			// working (sanity check)
			name:       "array of 60 elements in left bitmap, 1 internal container (numbers < 2^16), non-empty bitmap (other container) on left Or",
			leftBm:     genBitmap(970, 1_030),
			rightBm:    genBitmap(1_000, 4_000),
			nextLeftBm: genBitmap(71_970, 72_000),

			expectedElements: append(genSlice(970, 1_000), genSlice(71_970, 72_000)...),
		},
		{
			// returns 200x 65536 instead of 71800..71999
			name:       "array of 300 elements in left bitmap, 2 internal containers (numbers > 2^16), non-empty (other container) bitmap on left Or",
			leftBm:     genBitmap(71_800, 72_100),
			rightBm:    genBitmap(72_000, 75_000),
			nextLeftBm: genBitmap(800, 1_000),

			expectedElements: append(genSlice(800, 1_000), genSlice(71_800, 72_000)...),
		},
		{
			// working (sanity check)
			name:       "array of 300 elements in left bitmap, 1 internal container (numbers < 2^16), non-empty (other container) bitmap on left Or",
			leftBm:     genBitmap(800, 1_100),
			rightBm:    genBitmap(1_000, 4_000),
			nextLeftBm: genBitmap(71_800, 72_000),

			expectedElements: append(genSlice(800, 1_000), genSlice(71_800, 72_000)...),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			broken := tc.leftBm.Clone()
			broken.AndNot(tc.rightBm)

			failing := tc.nextLeftBm.Clone()
			failing.Or(broken)

			elements := failing.ToArray()

			require.Len(t, elements, len(tc.expectedElements))
			require.ElementsMatch(t, elements, tc.expectedElements)
		})
	}
}

func Test_Issue_2_ZeroInsteadMax(t *testing.T) {
	for maxVal := uint64(2); maxVal <= 512; maxVal++ {
		t.Run(fmt.Sprintf("max_%d", maxVal), func(t *testing.T) {
			bm1 := NewBitmap()
			bm2 := NewBitmap()

			for i := uint64(0); i <= maxVal-2; i++ {
				bm1.Set(i)
			}
			bm2.Set(maxVal)

			bm1.Or(bm2)
			bm1.Set(maxVal - 1)

			assert_.Equal(t, maxVal+1, uint64(bm1.GetCardinality()), "should have cardinality")
			assert_.Equal(t, maxVal, bm1.Maximum(), "should have maximum")
			assert_.Equal(t, uint64(0), bm1.Minimum(), "should have minimum")
			assert_.True(t, bm1.Contains(maxVal-2), "should contain maxVal-2")
			assert_.True(t, bm1.Contains(maxVal-1), "should contain maxVal-1")
			assert_.True(t, bm1.Contains(maxVal), "should contain maxVal")
		})

		t.Run(fmt.Sprintf("max_%d_sanity1", maxVal), func(t *testing.T) {
			bm1 := NewBitmap()

			for i := uint64(0); i <= maxVal; i++ {
				bm1.Set(i)
			}

			assert_.Equal(t, maxVal+1, uint64(bm1.GetCardinality()), "should have cardinality")
			assert_.Equal(t, maxVal, bm1.Maximum(), "should have maximum")
			assert_.Equal(t, uint64(0), bm1.Minimum(), "should have minimum")
			assert_.True(t, bm1.Contains(maxVal-2), "should contain maxVal-2")
			assert_.True(t, bm1.Contains(maxVal-1), "should contain maxVal-1")
			assert_.True(t, bm1.Contains(maxVal), "should contain maxVal")
		})

		t.Run(fmt.Sprintf("max_%d_sanity2", maxVal), func(t *testing.T) {
			bm1 := NewBitmap()
			bm2 := NewBitmap()

			for i := uint64(0); i <= maxVal-2; i++ {
				bm1.Set(i)
			}
			bm2.Set(maxVal)

			bm1.Set(maxVal - 1)
			bm1.Or(bm2)

			assert_.Equal(t, maxVal+1, uint64(bm1.GetCardinality()), "should have cardinality")
			assert_.Equal(t, maxVal, bm1.Maximum(), "should have maximum")
			assert_.Equal(t, uint64(0), bm1.Minimum(), "should have minimum")
			assert_.True(t, bm1.Contains(maxVal-2), "should contain maxVal-2")
			assert_.True(t, bm1.Contains(maxVal-1), "should contain maxVal-1")
			assert_.True(t, bm1.Contains(maxVal), "should contain maxVal")
		})
	}
}

func Test_Issue_2_OutOfRange(t *testing.T) {
	for maxVal := uint64(2); maxVal <= 512; maxVal++ {
		t.Run(fmt.Sprintf("max_%d", maxVal), func(t *testing.T) {
			errCh := make(chan error, 1)
			go func() {
				defer func() {
					r := recover()
					if r != nil {
						errCh <- fmt.Errorf("%v", r)
					} else {
						errCh <- nil
					}
				}()

				bm1 := NewBitmap()
				bm2 := NewBitmap()

				for i := uint64(0); i <= maxVal-2; i++ {
					bm1.Set(i)
				}
				bm2.Set(maxVal - 1)

				bm1.Or(bm2)
				bm1.Set(maxVal)

				assert_.Equal(t, maxVal+1, uint64(bm1.GetCardinality()), "should have cardinality")
				assert_.Equal(t, maxVal, bm1.Maximum(), "should have maximum")
				assert_.Equal(t, uint64(0), bm1.Minimum(), "should have minimum")
				assert_.True(t, bm1.Contains(maxVal-2), "should contain maxVal-2")
				assert_.True(t, bm1.Contains(maxVal-1), "should contain maxVal-1")
				assert_.True(t, bm1.Contains(maxVal), "should contain maxVal")
			}()

			assert_.NoError(t, <-errCh)
		})
	}
}

func TestReset(t *testing.T) {
	bmTemplate := NewBitmap()
	delta5 := uint64(maxCardinality / 5)
	delta7 := uint64(maxCardinality / 7)
	for i := uint64(0); i < 1000; i++ {
		bmTemplate.Set(i * delta5)
		bmTemplate.Set(i * delta7)
	}

	t.Run("empty after reset", func(t *testing.T) {
		bm := bmTemplate.Clone()

		bm.Reset()

		require.True(t, bm.IsEmpty())
		require.Equal(t, 1, bm.keys.numKeys())
		require.Equal(t, 2, bm.keys.maxKeys())
		require.Equal(t, 24, bm.keys.size())
		require.Greater(t, bmTemplate.LenInBytes(), bm.LenInBytes())
		require.Equal(t, bmTemplate.LenInBytes(), bm.capInBytes())
	})

	t.Run("no panic on merge after reset", func(t *testing.T) {
		// due to Bitmap::Reset method not setting keys.size properly
		// Bitmap::expandConditionally paniced with "slice bounds out of range [4664:1736]".
		// This test prevents regression.

		bm := bmTemplate.Clone()

		bm.Reset()
		bm.Or(bmTemplate)

		require.Equal(t, bmTemplate.GetCardinality(), bm.GetCardinality())
		require.ElementsMatch(t, bmTemplate.ToArray(), bm.ToArray())
	})

	t.Run("reused bitmap serializes like a fresh one", func(t *testing.T) {
		refilled := map[string][]uint64{
			"single value":     {1},
			"nothing":          {},
			"spanning keys":    {2, 3},
			"beyond first key": {1 << 16},
		}
		for name, vals := range refilled {
			t.Run(name, func(t *testing.T) {
				bm := bmTemplate.Clone()
				bm.Reset()
				fresh := NewBitmap()
				for _, v := range vals {
					bm.Set(v)
					fresh.Set(v)
				}

				require.Equal(t, fresh.ToArray(), bm.ToArray())
				require.Equal(t, fresh.ToBuffer(), bm.ToBuffer())
			})
		}
	})
}

func TestZeroOut(t *testing.T) {
	bmTemplate := NewBitmap()
	delta5 := uint64(maxCardinality / 5)
	delta7 := uint64(maxCardinality / 7)
	for i := uint64(0); i < 1000; i++ {
		bmTemplate.Set(i * delta5)
		bmTemplate.Set(i * delta7)
	}

	clone := func(template *Bitmap) *Bitmap {
		buf := make([]byte, 0, template.capInBytes())
		return template.CloneToBuf(buf)
	}

	t.Run("empty after zero out, no size has changed", func(t *testing.T) {
		bm := clone(bmTemplate)

		bm.ZeroOut()

		require.True(t, bm.IsEmpty())
		require.Equal(t, bmTemplate.keys.numKeys(), bm.keys.numKeys())
		require.Equal(t, bmTemplate.keys.maxKeys(), bm.keys.maxKeys())
		require.Equal(t, bmTemplate.keys.size(), bm.keys.size())
		require.Equal(t, bmTemplate.LenInBytes(), bm.LenInBytes())
		require.Equal(t, bmTemplate.capInBytes(), bm.capInBytes())
	})

	t.Run("repeated zero out leaves containers empty", func(t *testing.T) {
		bm := clone(bmTemplate)

		// Second pass hits zeroOutContainer's already-empty skip; a merge
		// afterwards surfaces any stale payload bits it might have left.
		bm.ZeroOut()
		bm.ZeroOut()

		require.True(t, bm.IsEmpty())
		bm.Or(bmTemplate)
		require.Equal(t, bmTemplate.GetCardinality(), bm.GetCardinality())
		require.ElementsMatch(t, bmTemplate.ToArray(), bm.ToArray())
	})

	t.Run("merge after zero out, no size has changed", func(t *testing.T) {
		bm := clone(bmTemplate)

		bm.ZeroOut()
		bm.Or(bmTemplate)

		require.Equal(t, bmTemplate.GetCardinality(), bm.GetCardinality())
		require.ElementsMatch(t, bmTemplate.ToArray(), bm.ToArray())
		require.Equal(t, bmTemplate.keys.numKeys(), bm.keys.numKeys())
		require.Equal(t, bmTemplate.keys.maxKeys(), bm.keys.maxKeys())
		require.Equal(t, bmTemplate.keys.size(), bm.keys.size())
		require.Equal(t, bmTemplate.LenInBytes(), bm.LenInBytes())
		require.Equal(t, bmTemplate.capInBytes(), bm.capInBytes())
	})
}
