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

func TestOrArrayAltMergesInPlace(t *testing.T) {
	tcs := []struct {
		name string
		slot int
		c, o []uint16
	}{
		{"no duplicates, interleaved", 64, uint16Range(0, 40, 2), uint16Range(1, 40, 2)},
		{"duplicates shift result down", 64, uint16Range(0, 30, 1), uint16Range(10, 40, 1)},
		{"other entirely below c", 64, uint16Range(100, 120, 1), uint16Range(0, 20, 1)},
		{"other entirely above c", 64, uint16Range(0, 20, 1), uint16Range(100, 120, 1)},
		{"other inside c, all duplicates", 64, uint16Range(0, 30, 1), uint16Range(5, 15, 1)},
		{"cnum is 1, below other", 64, []uint16{0}, uint16Range(10, 20, 1)},
		{"cnum is 1, above other", 64, []uint16{65535}, uint16Range(10, 20, 1)},
		{"cnum is 1, duplicate", 64, []uint16{15}, uint16Range(10, 20, 1)},
		{"onum is 1, below c", 64, uint16Range(10, 20, 1), []uint16{0}},
		{"onum is 1, above c", 64, uint16Range(10, 20, 1), []uint16{65535}},
		{"onum is 1, duplicate", 64, uint16Range(10, 20, 1), []uint16{15}},
		{"union exactly fills slot", 64, uint16Range(0, 60, 2), uint16Range(1, 60, 2)},
		{"cnum+onum fills slot, duplicates", 64, uint16Range(0, 30, 1), uint16Range(29, 59, 1)},
		{"cnum+onum one past slot, union fits", 64, uint16Range(0, 40, 1), uint16Range(30, 51, 1)},
		{"legacy slot between array and bitmap size", 3000, uint16Range(0, 3000, 2), uint16Range(1, 1800, 2)},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			c := arrayContainer(tc.slot, tc.c)
			other := arrayContainer(int(startIdx)+len(tc.o), tc.o)

			buf := make([]uint16, maxContainerSize)

			require.Nil(t, containerOrAlt(c, other, buf, runInline))

			if int(startIdx)+len(tc.c)+len(tc.o) <= tc.slot {
				require.Equal(t, make([]uint16, maxContainerSize), buf, "merge in place leaves buf untouched")
			}
			require.Equal(t, uint16(tc.slot), c[indexSize])
			require.Equal(t, typeArray, c[indexType])
			require.Equal(t, sortedUnion(tc.c, tc.o), c.all())
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
