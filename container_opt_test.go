package sroar

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainerExtremes(t *testing.T) {
	bitmapYs := []int{
		0, 1, 15, 16, 17, 1022, 1023, 1024, 1025,
		maxCardinality/2 - 1, maxCardinality / 2, maxCardinality/2 + 1,
		maxCardinality - 3, maxCardinality - 2, maxCardinality - 1,
	}

	t.Run("bitmap maximum", func(t *testing.T) {
		b := bitmap(make([]uint16, maxContainerSize))

		for i := 0; i < len(bitmapYs); i++ {
			y := uint16(bitmapYs[i])
			t.Run(fmt.Sprint(y), func(t *testing.T) {
				b.add(y)
				require.Equal(t, y, b.maximum())
			})
		}
	})

	t.Run("bitmap minimum", func(t *testing.T) {
		b := bitmap(make([]uint16, maxContainerSize))

		for i := len(bitmapYs) - 1; i >= 0; i-- {
			y := uint16(bitmapYs[i])
			t.Run(fmt.Sprint(y), func(t *testing.T) {
				b.add(y)
				require.Equal(t, y, b.minimum())
			})
		}
	})
}

func arrayContainer(slot int, vals []uint16) array {
	c := make([]uint16, slot)
	c[indexSize] = uint16(slot)
	c[indexType] = typeArray
	setCardinality(c, len(vals))
	copy(c[startIdx:], vals)
	return c
}

func uint16Range(from, to, step int) []uint16 {
	var vals []uint16
	for v := from; v < to; v += step {
		vals = append(vals, uint16(v))
	}
	return vals
}

func sortedUnion(a, b []uint16) []uint16 {
	out := append(append([]uint16{}, a...), b...)
	slices.Sort(out)
	return slices.Compact(out)
}

func TestOrArrayAltResult(t *testing.T) {
	const (
		keptArray     = iota // array in c's slot
		keptBitmap           // bitmap in c's slot
		newArray             // array returned in buf
		newBitmap            // bitmap returned in buf
		returnedC            // c itself returned
		returnedOther        // other itself returned
	)
	tcs := []struct {
		name    string
		slot    int
		runMode int
		c, o    []uint16
		want    int
		newSize int
	}{
		{"no duplicates, interleaved", 64, runInline, uint16Range(0, 40, 2), uint16Range(1, 40, 2), keptArray, 0},
		{"duplicates shift result down", 64, runInline, uint16Range(0, 30, 1), uint16Range(10, 40, 1), keptArray, 0},
		{"other entirely below c", 64, runInline, uint16Range(100, 120, 1), uint16Range(0, 20, 1), keptArray, 0},
		{"other entirely above c", 64, runInline, uint16Range(0, 20, 1), uint16Range(100, 120, 1), keptArray, 0},
		{"other inside c, all duplicates", 64, runInline, uint16Range(0, 30, 1), uint16Range(5, 15, 1), keptArray, 0},
		{"cnum is 1, below other", 64, runInline, []uint16{0}, uint16Range(10, 20, 1), keptArray, 0},
		{"cnum is 1, above other", 64, runInline, []uint16{65535}, uint16Range(10, 20, 1), keptArray, 0},
		{"cnum is 1, duplicate", 64, runInline, []uint16{15}, uint16Range(10, 20, 1), keptArray, 0},
		{"onum is 1, below c", 64, runInline, uint16Range(10, 20, 1), []uint16{0}, keptArray, 0},
		{"onum is 1, above c", 64, runInline, uint16Range(10, 20, 1), []uint16{65535}, keptArray, 0},
		{"onum is 1, duplicate", 64, runInline, uint16Range(10, 20, 1), []uint16{15}, keptArray, 0},
		{"union exactly fills slot", 64, runInline, uint16Range(0, 60, 2), uint16Range(1, 60, 2), keptArray, 0},
		{"cnum+onum fills slot, duplicates", 64, runInline, uint16Range(0, 30, 1), uint16Range(29, 59, 1), keptArray, 0},
		{"cnum+onum one past slot, union fits", 64, runInline, uint16Range(0, 40, 1), uint16Range(30, 51, 1), keptArray, 0},
		{"union too large for slot", 64, runInline, uint16Range(0, 40, 1), uint16Range(40, 80, 1), newArray, 128},
		{"legacy slot between array and bitmap size", 3000, runInline, uint16Range(0, 3000, 2), uint16Range(1, 1800, 2), keptArray, 0},

		{"full slot, union of a lower subset", 2004, runInline, uint16Range(0, 2000, 1), uint16Range(0, 1500, 1), keptArray, 0},
		{"full slot, union of an upper subset", 2004, runInline, uint16Range(0, 2000, 1), uint16Range(500, 2000, 1), keptArray, 0},
		{"bitmap-sized slot, array-sized union", maxContainerSize, runInline, uint16Range(0, 1500, 1), uint16Range(544, 2044, 1), keptBitmap, 0},
		{"bitmap-sized slot, few values", maxContainerSize, runInline, uint16Range(0, 30, 1), uint16Range(20, 50, 1), keptBitmap, 0},
		{"bitmap-sized slot, union one past array size", maxContainerSize, runInline, uint16Range(0, 1500, 1), uint16Range(545, 2045, 1), keptBitmap, 0},
		{"small slot, array-sized union", 1504, runInline, uint16Range(0, 1500, 1), uint16Range(544, 2044, 1), newArray, 2048},
		{"small slot, union one past array size", 1504, runInline, uint16Range(0, 1500, 1), uint16Range(545, 2045, 1), newBitmap, maxContainerSize},
		{"legacy slot fits union past array size", 3000, runInline, uint16Range(0, 1500, 1), uint16Range(500, 2500, 1), keptArray, 0},
		{"legacy array past array size, union fits slot", 3500, runInline, uint16Range(0, 3000, 1), uint16Range(2200, 3400, 1), keptArray, 0},
		{"legacy array past array size, union past slot", 3500, runInline, uint16Range(0, 3000, 1), uint16Range(2400, 3600, 1), newBitmap, maxContainerSize},
		{"legacy arrays, union past buf's array capacity", 3500, runInline, uint16Range(0, 3000, 1), uint16Range(3000, 4500, 1), newBitmap, maxContainerSize},
		{"legacy array past array size, bitmap-sized slot", maxContainerSize, runInline, uint16Range(0, 3000, 1), uint16Range(2200, 3400, 1), keptBitmap, 0},

		{"c empty, fits slot", 64, runInline, []uint16{}, uint16Range(0, 50, 1), keptArray, 0},
		{"c empty, array-sized other", 64, runInline, []uint16{}, uint16Range(0, 2044, 1), returnedOther, 0},
		{"c empty, legacy other past array size", 64, runInline, []uint16{}, uint16Range(0, 2500, 1), newArray, maxContainerSize},
		{"c empty in bitmap-sized slot, legacy other past array size", maxContainerSize, runInline, []uint16{}, uint16Range(0, 2500, 1), keptArray, 0},
		{"other empty, legacy c in bitmap-sized slot", maxContainerSize, runInline, uint16Range(0, 2500, 1), []uint16{}, keptArray, 0},
		{"other empty, legacy c in legacy slot", 3000, runInline, uint16Range(0, 2500, 1), []uint16{}, keptArray, 0},

		{"not inline, small union", 64, 0, uint16Range(0, 30, 1), uint16Range(10, 40, 1), newArray, 64},
		{"not inline, array-sized union", maxContainerSize, 0, uint16Range(0, 1500, 1), uint16Range(544, 2044, 1), newArray, 2048},
		{"not inline, union one past array size", maxContainerSize, 0, uint16Range(0, 1500, 1), uint16Range(545, 2045, 1), newBitmap, maxContainerSize},
		{"not inline, legacy arrays past array size", 3500, 0, uint16Range(0, 3000, 1), uint16Range(2200, 3400, 1), newBitmap, maxContainerSize},
		{"not inline, c empty", 64, 0, []uint16{}, uint16Range(0, 60, 1), returnedOther, 0},
		{"not inline, other empty, spare room in c", 2048, 0, uint16Range(0, 30, 1), []uint16{}, newArray, 64},
		{"not inline, other empty, right-sized c", 64, 0, uint16Range(0, 30, 1), []uint16{}, returnedC, 0},
		{"not inline, other empty, legacy c", 3000, 0, uint16Range(0, 2500, 1), []uint16{}, newArray, maxContainerSize},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			c := arrayContainer(tc.slot, tc.c)
			other := arrayContainer(int(startIdx)+len(tc.o), tc.o)
			buf := make([]uint16, maxContainerSize)

			got := containerOrAlt(c, other, buf, tc.runMode)

			res, wantSize := []uint16(c), uint16(tc.slot)
			switch tc.want {
			case keptArray, keptBitmap:
				require.Nil(t, got)
			case newArray, newBitmap:
				require.NotEmpty(t, got)
				require.Same(t, &buf[0], &got[0], "callers swap buffers, so a new container starts at buf")
				require.Equal(t, tc.c, c.all(), "c is unchanged when the result is returned")
				res, wantSize = got, uint16(tc.newSize)
			case returnedC:
				require.Same(t, &c[0], &got[0])
				res = got
			case returnedOther:
				require.Same(t, &other[0], &got[0])
				require.Equal(t, tc.c, c.all())
				res, wantSize = got, uint16(len(other))
			}
			wantType := typeArray
			switch tc.want {
			case keptBitmap, newBitmap:
				wantType = typeBitmap
			}

			if tc.runMode&runInline != 0 && int(startIdx)+len(tc.c)+len(tc.o) <= tc.slot && tc.slot != maxContainerSize {
				require.Equal(t, make([]uint16, maxContainerSize), buf, "merge in place leaves buf untouched")
			}
			require.Equal(t, wantType, res[indexType])
			require.Equal(t, wantSize, res[indexSize])
			require.Len(t, res, int(wantSize))
			want := sortedUnion(tc.c, tc.o)
			require.Equal(t, len(want), getCardinality(res))
			if wantType == typeArray {
				require.Equal(t, want, array(res).all())
			} else {
				require.Equal(t, want, bitmap(res).all())
			}
			require.Equal(t, tc.o, other.all())
		})
	}

	t.Run("same container on both sides", func(t *testing.T) {
		vals := uint16Range(0, 20, 1)
		c := arrayContainer(64, vals)

		require.Nil(t, containerOrAlt(c, c, make([]uint16, maxContainerSize), runInline))

		require.Equal(t, vals, c.all())
		require.Equal(t, make([]uint16, 64-int(startIdx)-len(vals)), []uint16(c[int(startIdx)+len(vals):]))
	})
}

// An inline array ∪ bitmap whose union is the bitmap, and outgrows the array's slot, returns the bitmap itself.
func TestOrArrayBitmapAltReturnsOther(t *testing.T) {
	tcs := []struct {
		name string
		c    []uint16
		o    []uint16
	}{
		{"c empty", []uint16{}, uint16Range(0, 3000, 1)},
		{"other full", uint16Range(0, 10, 1), uint16Range(0, maxCardinality, 1)},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			c := arrayContainer(64, tc.c)
			other := arrayContainer(int(startIdx)+len(tc.o), tc.o).toBitmapContainer(nil)
			buf := make([]uint16, maxContainerSize)

			got := containerOrAlt(c, other, buf, runInline)

			require.Same(t, &other[0], &got[0])
			require.Equal(t, tc.c, array(c).all())
		})
	}
}

// Values fed in several descending chunks merge the same as in one call.
func TestUnion2by2BackwardChunks(t *testing.T) {
	set1 := uint16Range(0, 60, 3)
	set2 := uint16Range(0, 60, 2)
	end := len(set1) + len(set2)
	data := make([]uint16, end)
	copy(data, set1)

	n1, pos := len(set1), end
	for hi := len(set2); hi > 0; hi -= 7 {
		n1, pos = union2by2Backward(data, n1, pos, set2[max(hi-7, 0):hi])
	}
	dups := pos - n1
	copy(data[n1:], data[pos:end])

	require.Equal(t, sortedUnion(set1, set2), data[:end-dups])
}
