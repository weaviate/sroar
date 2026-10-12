package sroar

import (
	"fmt"
	"math"
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

const (
	orKeptArray     = iota // array in c's slot
	orKeptBitmap           // bitmap in c's slot
	orNewArray             // array returned in buf
	orNewBitmap            // bitmap returned in buf
	orReturnedC            // c itself returned
	orReturnedOther        // other itself returned
)

type orResultCase struct {
	name    string
	slot    int
	runMode int
	c, o    []uint16
	want    int
	newSize int
}

// testOrResult runs containerOrAlt on an array c in a slot of tc.slot and
// other built by newOther, and checks where and how the union is stored.
func testOrResult(t *testing.T, tc orResultCase, newOther func(vals []uint16) []uint16, bufUntouched bool) {
	c := arrayContainer(tc.slot, tc.c)
	other := newOther(tc.o)
	buf := make([]uint16, maxContainerSize)

	got := containerOrAlt(c, other, buf, tc.runMode)

	res, wantSize, wantType := []uint16(c), uint16(tc.slot), typeArray
	switch tc.want {
	case orKeptArray, orKeptBitmap:
		require.Nil(t, got)
	case orNewArray, orNewBitmap:
		require.NotEmpty(t, got)
		require.Same(t, &buf[0], &got[0], "callers swap buffers, so a new container starts at buf")
		require.Equal(t, tc.c, c.all(), "c is unchanged when the result is returned")
		res, wantSize = got, uint16(tc.newSize)
	case orReturnedC:
		require.Same(t, &c[0], &got[0])
		res = got
	case orReturnedOther:
		require.Same(t, &other[0], &got[0])
		require.Equal(t, tc.c, c.all())
		res, wantSize, wantType = got, uint16(len(other)), other[indexType]
	}
	switch tc.want {
	case orKeptBitmap, orNewBitmap:
		wantType = typeBitmap
	}

	if bufUntouched {
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
	if other[indexType] == typeArray {
		require.Equal(t, tc.o, array(other).all())
	} else {
		require.Equal(t, tc.o, bitmap(other).all())
	}
}

func TestOrArrayAltResult(t *testing.T) {
	tcs := []orResultCase{
		{"no duplicates, interleaved", 64, runInline, uint16Range(0, 40, 2), uint16Range(1, 40, 2), orKeptArray, 0},
		{"duplicates shift result down", 64, runInline, uint16Range(0, 30, 1), uint16Range(10, 40, 1), orKeptArray, 0},
		{"other entirely below c", 64, runInline, uint16Range(100, 120, 1), uint16Range(0, 20, 1), orKeptArray, 0},
		{"other entirely above c", 64, runInline, uint16Range(0, 20, 1), uint16Range(100, 120, 1), orKeptArray, 0},
		{"other inside c, all duplicates", 64, runInline, uint16Range(0, 30, 1), uint16Range(5, 15, 1), orKeptArray, 0},
		{"cnum is 1, below other", 64, runInline, []uint16{0}, uint16Range(10, 20, 1), orKeptArray, 0},
		{"cnum is 1, above other", 64, runInline, []uint16{65535}, uint16Range(10, 20, 1), orKeptArray, 0},
		{"cnum is 1, duplicate", 64, runInline, []uint16{15}, uint16Range(10, 20, 1), orKeptArray, 0},
		{"onum is 1, below c", 64, runInline, uint16Range(10, 20, 1), []uint16{0}, orKeptArray, 0},
		{"onum is 1, above c", 64, runInline, uint16Range(10, 20, 1), []uint16{65535}, orKeptArray, 0},
		{"onum is 1, duplicate", 64, runInline, uint16Range(10, 20, 1), []uint16{15}, orKeptArray, 0},
		{"union exactly fills slot", 64, runInline, uint16Range(0, 60, 2), uint16Range(1, 60, 2), orKeptArray, 0},
		{"cnum+onum fills slot, duplicates", 64, runInline, uint16Range(0, 30, 1), uint16Range(29, 59, 1), orKeptArray, 0},
		{"cnum+onum one past slot, union fits", 64, runInline, uint16Range(0, 40, 1), uint16Range(30, 51, 1), orKeptArray, 0},
		{"union too large for slot", 64, runInline, uint16Range(0, 40, 1), uint16Range(40, 80, 1), orNewArray, 128},
		{"legacy slot between array and bitmap size", 3000, runInline, uint16Range(0, 3000, 2), uint16Range(1, 1800, 2), orKeptArray, 0},

		{"full slot, union of a lower subset", 2004, runInline, uint16Range(0, 2000, 1), uint16Range(0, 1500, 1), orKeptArray, 0},
		{"full slot, union of an upper subset", 2004, runInline, uint16Range(0, 2000, 1), uint16Range(500, 2000, 1), orKeptArray, 0},
		{"bitmap-sized slot, array-sized union", maxContainerSize, runInline, uint16Range(0, 1500, 1), uint16Range(544, 2044, 1), orKeptBitmap, 0},
		{"bitmap-sized slot, few values", maxContainerSize, runInline, uint16Range(0, 30, 1), uint16Range(20, 50, 1), orKeptBitmap, 0},
		{"bitmap-sized slot, union one past array size", maxContainerSize, runInline, uint16Range(0, 1500, 1), uint16Range(545, 2045, 1), orKeptBitmap, 0},
		{"small slot, array-sized union", 1504, runInline, uint16Range(0, 1500, 1), uint16Range(544, 2044, 1), orNewArray, 2048},
		{"small slot, union one past array size", 1504, runInline, uint16Range(0, 1500, 1), uint16Range(545, 2045, 1), orNewBitmap, maxContainerSize},
		{"legacy slot fits union past array size", 3000, runInline, uint16Range(0, 1500, 1), uint16Range(500, 2500, 1), orKeptArray, 0},
		{"legacy array past array size, union fits slot", 3500, runInline, uint16Range(0, 3000, 1), uint16Range(2200, 3400, 1), orKeptArray, 0},
		{"legacy array past array size, union past slot", 3500, runInline, uint16Range(0, 3000, 1), uint16Range(2400, 3600, 1), orNewBitmap, maxContainerSize},
		{"legacy arrays, union past buf's array capacity", 3500, runInline, uint16Range(0, 3000, 1), uint16Range(3000, 4500, 1), orNewBitmap, maxContainerSize},
		{"legacy array past array size, bitmap-sized slot", maxContainerSize, runInline, uint16Range(0, 3000, 1), uint16Range(2200, 3400, 1), orKeptBitmap, 0},

		{"c empty, fits slot", 64, runInline, []uint16{}, uint16Range(0, 50, 1), orKeptArray, 0},
		{"c empty, other exactly fills slot", 64, runInline, []uint16{}, uint16Range(0, 60, 1), orKeptArray, 0},
		{"c empty, other one past slot", 64, runInline, []uint16{}, uint16Range(0, 61, 1), orNewArray, 128},
		{"c empty, legacy other past array size", 64, runInline, []uint16{}, uint16Range(0, 2500, 1), orNewArray, maxContainerSize},
		{"c empty, right-sized other past slot", 64, runInline, []uint16{}, uint16Range(0, 124, 1), orReturnedOther, 0},
		{"c empty in bitmap-sized slot, legacy other past array size", maxContainerSize, runInline, []uint16{}, uint16Range(0, 2500, 1), orKeptArray, 0},
		{"other empty, legacy c in bitmap-sized slot", maxContainerSize, runInline, uint16Range(0, 2500, 1), []uint16{}, orKeptArray, 0},
		{"other empty, legacy c in legacy slot", 3000, runInline, uint16Range(0, 2500, 1), []uint16{}, orKeptArray, 0},
		{"both empty", 64, runInline, []uint16{}, []uint16{}, orKeptArray, 0},

		{"bitmap-sized slot, disjoint ranges past array size", maxContainerSize, runInline, uint16Range(0, 1100, 1), uint16Range(2000, 3000, 1), orKeptBitmap, 0},
		{"edge bounds, run with other sticking out past array size", 1504, runInline, uint16Range(0, 1500, 1), uint16Range(1000, 3200, 2), orNewBitmap, maxContainerSize},
		{"bitmap-sized slot, interleaved past array size", maxContainerSize, runInline, uint16Range(0, 3000, 2), uint16Range(1, 3000, 2), orKeptBitmap, 0},
		{"edge bounds undecided, union array-sized", 1504, runInline, uint16Range(0, 3000, 2), append([]uint16{0}, uint16Range(1000, 1600, 1)...), orNewArray, 2048},
		{"edge bounds undecided, union past array size", 2048, 0, uint16Range(0, 3000, 2), uint16Range(1, 3000, 2), orNewBitmap, maxContainerSize},
		{"edge bounds undecided, union past array size, inline", 1504, runInline, uint16Range(0, 3000, 2), uint16Range(1, 3000, 2), orNewBitmap, maxContainerSize},
		{"legacy interleaved arrays, union past buf", 3008, runInline, uint16Range(0, 6000, 2), uint16Range(1, 6000, 2), orNewBitmap, maxContainerSize},

		{"not inline, small union", 64, 0, uint16Range(0, 30, 1), uint16Range(10, 40, 1), orNewArray, 64},
		{"not inline, array-sized union", maxContainerSize, 0, uint16Range(0, 1500, 1), uint16Range(544, 2044, 1), orNewArray, 2048},
		{"not inline, union one past array size", maxContainerSize, 0, uint16Range(0, 1500, 1), uint16Range(545, 2045, 1), orNewBitmap, maxContainerSize},
		{"not inline, legacy arrays past array size", 3500, 0, uint16Range(0, 3000, 1), uint16Range(2200, 3400, 1), orNewBitmap, maxContainerSize},
		{"not inline, edge bounds past array size", 2048, 0, uint16Range(0, 3000, 2), uint16Range(2000, 5000, 2), orNewBitmap, maxContainerSize},
		{"not inline, edge bounds undecided, union array-sized", 2048, 0, uint16Range(0, 3000, 2), uint16Range(1000, 3200, 2), orNewArray, 2048},
		{"not inline, c empty", 64, 0, []uint16{}, uint16Range(0, 60, 1), orReturnedOther, 0},
		{"not inline, other empty, spare room in c", 2048, 0, uint16Range(0, 30, 1), []uint16{}, orNewArray, 64},
		{"not inline, other empty, right-sized c", 64, 0, uint16Range(0, 30, 1), []uint16{}, orReturnedC, 0},
		{"not inline, other empty, legacy c", 3000, 0, uint16Range(0, 2500, 1), []uint16{}, orNewArray, maxContainerSize},
		{"not inline, both empty", 64, 0, []uint16{}, []uint16{}, orReturnedC, 0},
	}
	newOther := func(vals []uint16) []uint16 { return arrayContainer(int(startIdx)+len(vals), vals) }
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			inPlace := tc.runMode&runInline != 0 && int(startIdx)+len(tc.c)+len(tc.o) <= tc.slot && tc.slot != maxContainerSize
			testOrResult(t, tc, newOther, inPlace)
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

func TestUnionMayFitArray(t *testing.T) {
	run := func(from, to int) []uint16 { return uint16Range(from, to, 1) }
	evens := func(from, to int) []uint16 { return uint16Range(from, to, 2) }
	tcs := []struct {
		name    string
		slot    int
		runMode int
		c, o    []uint16
		want    bool
	}{
		{"disjoint ranges, c below", 2048, 0, run(0, 1100), run(2000, 3000), false},
		{"disjoint ranges, c above", 2048, 0, run(2000, 3000), run(0, 1100), false},
		{"disjoint ranges, legacy slot keeps the union", 3004, runInline, run(0, 1250), run(2000, 3250), true},
		{"c sticks out low, decided", 2048, 0, evens(0, 3000), evens(2000, 5000), false},
		{"other sticks out low, decided", 2048, 0, evens(2000, 5000), evens(0, 3000), false},
		{"other sticks out at both ends, decided", 2048, 0, run(1000, 1500), evens(0, 4000), false},
		{"other sticks out at both ends, undecided", 2048, 0, evens(1000, 3000), evens(0, 4000), true},
		{"each sticks out at one end, decided only by other's low end", 2048, 0,
			append(run(200, 2000), evens(2000, 2100)...), append(run(0, 200), uint16Range(200, 2000, 50)...), false},
		{"each sticks out at one end, decided only by c's low end", 2048, 0,
			append(run(0, 200), uint16Range(200, 2000, 50)...), append(run(200, 2000), evens(2000, 2100)...), false},
		{"each sticks out at one end, decided only by other's high end", 2048, 0,
			append(evens(0, 100), run(100, 1900)...), append(uint16Range(100, 1900, 50), run(1900, 2100)...), false},
		{"equal first values, other sticks out high", 2048, 0, evens(0, 3000), append([]uint16{0}, evens(3000, 5000)...), false},
		{"equal first values, undecided", 2048, 0, evens(0, 3000), append([]uint16{0}, evens(1000, 4000)...), true},
		{"equal at both ends, undecided", 2048, 0, evens(0, 3000), uint16Range(0, 3000, 3), true},
		{"interleaved disjoint, undecided", 2048, 0, evens(0, 3000), evens(1, 3000), true},

		{"c a run, union of 2044", 2048, 0, run(0, 1500), evens(1000, 2588), true},
		{"c a run, union of 2045", 2048, 0, run(0, 1500), evens(1000, 2590), false},
		{"other a run, union of 2044", 2048, 0, evens(1000, 2588), run(0, 1500), true},
		{"other a run, union of 2045", 2048, 0, evens(1000, 2590), run(0, 1500), false},

		{"inline, bitmap-sized slot", maxContainerSize, runInline, evens(0, 3000), evens(2000, 5000), false},
		{"inline, small slot", 1504, runInline, evens(0, 3000), evens(2000, 5000), false},
		{"inline, legacy slot keeps bound", 2804, runInline, evens(0, 3000), evens(2000, 5000), true},
		{"inline, legacy slot past bound", 2404, runInline, evens(0, 3000), evens(1000, 5000), false},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			c := arrayContainer(tc.slot, tc.c)
			other := arrayContainer(int(startIdx)+len(tc.o), tc.o)
			require.False(t, isArrayCard(len(tc.c)+len(tc.o)), "shapes below route the merge before the bound")

			got := c.unionMayFitArray(other, tc.runMode)

			require.Equal(t, tc.want, got)
			u := len(sortedUnion(tc.c, tc.o))
			if !got {
				require.False(t, isArrayCard(u) || tc.runMode&runInline != 0 && c.canFitArray(u), "ruling out an array must hold for the real union")
			}
		})
	}
}

func TestOrArrayBitmapAltResult(t *testing.T) {
	run := func(from, to int) []uint16 { return uint16Range(from, to, 1) }
	full := run(0, maxCardinality)
	tcs := []orResultCase{
		{"interleaved, fits slot", 128, runInline, uint16Range(0, 60, 2), uint16Range(1, 60, 2), orKeptArray, 0},
		{"c entirely below other", 128, runInline, run(0, 30), run(1000, 1050), orKeptArray, 0},
		{"c entirely above other", 128, runInline, run(60000, 60030), run(0, 50), orKeptArray, 0},
		{"heavy overlap, union exactly fills slot", 64, runInline, run(0, 60), uint16Range(0, 60, 2), orKeptArray, 0},
		{"other inside c, all duplicates", 64, runInline, run(0, 60), run(10, 20), orKeptArray, 0},
		{"values spread over the whole range", 2048, runInline, uint16Range(3, maxCardinality, 97), uint16Range(0, maxCardinality, 64), orKeptArray, 0},
		{"union one past slot", 64, runInline, run(0, 30), run(100, 131), orNewBitmap, maxContainerSize},
		{"bitmap-sized slot, union of 2044", maxContainerSize, runInline, run(0, 1500), run(544, 2044), orKeptBitmap, 0},
		{"bitmap-sized slot, few values", maxContainerSize, runInline, run(0, 30), run(100, 140), orKeptBitmap, 0},
		{"bitmap-sized slot, union of 2045", maxContainerSize, runInline, run(0, 1500), run(545, 2045), orKeptBitmap, 0},
		{"small slot, union of 2044", 1504, runInline, run(0, 1500), run(544, 2044), orNewBitmap, maxContainerSize},
		{"small slot, union of 2045", 1504, runInline, run(0, 1500), run(545, 2045), orNewBitmap, maxContainerSize},
		{"legacy slot fits union past array size", 3004, runInline, run(0, 1500), run(500, 3000), orKeptArray, 0},
		{"legacy slot fits other, not the union", 3004, runInline, run(0, 1500), run(1000, 3100), orNewBitmap, maxContainerSize},
		{"legacy slot smaller than other", 2504, runInline, run(2900, 3100), run(0, 3000), orNewBitmap, maxContainerSize},
		{"legacy array past array size, union fits slot", 3504, runInline, run(0, 3000), run(2200, 3400), orKeptArray, 0},
		{"legacy array past array size, bitmap-sized slot", maxContainerSize, runInline, run(0, 3000), run(2200, 3400), orKeptBitmap, 0},
		{"bitmap-sized slot, other past array size", maxContainerSize, runInline, run(0, 100), run(50, 30050), orKeptBitmap, 0},

		{"other empty", 64, runInline, run(0, 10), []uint16{}, orKeptArray, 0},
		{"c empty", 64, runInline, []uint16{}, run(0, 3000), orReturnedOther, 0},
		{"c empty, other with few values", 64, runInline, []uint16{}, run(0, 10), orKeptArray, 0},
		{"c empty, other exactly fills slot", 64, runInline, []uint16{}, run(0, 60), orKeptArray, 0},
		{"c empty, other one past slot", 64, runInline, []uint16{}, run(0, 61), orReturnedOther, 0},
		{"c empty in legacy slot, other past array size", 3004, runInline, []uint16{}, run(0, 2500), orKeptArray, 0},
		{"c empty in bitmap-sized slot, other with few values", maxContainerSize, runInline, []uint16{}, run(0, 10), orKeptBitmap, 0},
		{"other full", 64, runInline, run(0, 10), full, orReturnedOther, 0},
		{"other full, bitmap-sized slot", maxContainerSize, runInline, run(0, 10), full, orKeptBitmap, 0},

		{"not inline, small union", 2048, 0, run(0, 30), run(10, 40), orNewBitmap, maxContainerSize},
		{"not inline, c entirely above other", 64, 0, run(60000, 60010), run(0, 40), orNewBitmap, maxContainerSize},
		{"not inline, union of 2044", maxContainerSize, 0, run(0, 1500), run(544, 2044), orNewBitmap, maxContainerSize},
		{"not inline, union of 2045", maxContainerSize, 0, run(0, 1500), run(545, 2045), orNewBitmap, maxContainerSize},
		{"not inline, legacy array past array size", 3504, 0, run(0, 3000), run(2200, 3400), orNewBitmap, maxContainerSize},
		{"not inline, other past array size", 64, 0, run(0, 10), run(5, 30005), orNewBitmap, maxContainerSize},
		{"not inline, other empty, spare room in c", 2048, 0, run(0, 10), []uint16{}, orNewArray, 64},
		{"not inline, other empty, right-sized c", 64, 0, run(0, 10), []uint16{}, orReturnedC, 0},
		{"not inline, c empty", 64, 0, []uint16{}, run(0, 10), orReturnedOther, 0},
		{"not inline, other full", 64, 0, run(0, 10), full, orReturnedOther, 0},
	}
	newOther := func(vals []uint16) []uint16 {
		return arrayContainer(int(startIdx)+len(vals), vals).toBitmapContainer(nil)
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			testOrResult(t, tc, newOther, false)
		})
	}
}

// bitmapWords returns a headerless run of bitmap words holding vals.
func bitmapWords(vals []uint16) []uint16 {
	words := make([]uint16, maxContainerSize-startIdx)
	for _, v := range vals {
		setBit(words, v)
	}
	return words
}

// bitsIntoArray matches the uint16 loop on unaligned windows and short outs.
func TestBitsIntoArrayMatchesUint16Loop(t *testing.T) {
	tcs := []struct {
		name string
		vals []uint16
	}{
		{"empty", nil},
		{"single first", []uint16{0}},
		{"single mid", []uint16{12345}},
		{"single last", []uint16{math.MaxUint16}},
		{"clustered", uint16Range(1000, 4000, 1)},
		{"spread", uint16Range(5, maxCardinality, 29)},
		{"full", uint16Range(0, maxCardinality, 1)},
	}
	check := func(t *testing.T, words []uint16, outLen int, baseWord uint16) {
		want := make([]uint16, outLen)
		got := make([]uint16, outLen)
		wantN := bitsIntoArray16(words, want, baseWord)
		gotN := bitsIntoArray(words, got, baseWord)
		require.Equal(t, wantN, gotN)
		require.Equal(t, want, got)
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			words := bitmapWords(tc.vals)
			n := len(tc.vals)

			t.Run("whole run", func(t *testing.T) {
				check(t, words, n, 0)
			})
			t.Run("short out", func(t *testing.T) {
				for _, outLen := range []int{0, 1, 2, 63, 64, 65, n / 3, n - 1} {
					if outLen >= 0 && outLen <= n {
						check(t, words, outLen, 0)
					}
				}
			})
			t.Run("unaligned windows", func(t *testing.T) {
				for _, lo := range []int{0, 1, 2, 3, 5, 62, 63, 250} {
					for _, hi := range []int{lo, lo + 1, lo + 3, lo + 4, lo + 37, len(words) - 2, len(words)} {
						if hi < lo || hi > len(words) {
							continue
						}
						check(t, words[lo:hi], n, uint16(lo))
						check(t, words[lo:hi], n/2, uint16(lo))
					}
				}
			})
		})
	}
}

// orValues and its mask loop match the per-value loop in new-value count and bits.
func TestOrValuesMatchesPerValueLoop(t *testing.T) {
	clustered := uint16Range(1000, 3000, 1)
	spread := uint16Range(5, maxCardinality, 61)
	tcs := []struct {
		name         string
		inBitmap     []uint16
		vals         []uint16
		wantNewCount int
	}{
		{"empty vals", clustered, nil, 0},
		{"clustered into empty", nil, clustered, 2000},
		{"spread into empty", nil, spread, len(spread)},
		{"clustered disjoint", spread[:16], uint16Range(1100, 1150, 1), 50},
		{"clustered overlapping", uint16Range(1000, 3000, 2), clustered, 1000},
		{"clustered all present", clustered, uint16Range(1500, 2500, 1), 0},
		{"spread disjoint", clustered, uint16Range(4000, maxCardinality, 59), len(uint16Range(4000, maxCardinality, 59))},
		{"spread overlapping", uint16Range(5, maxCardinality, 122), spread, len(spread) / 2},
		{"word edges", uint16Range(0, maxCardinality, 64), []uint16{0, 15, 16, 63, 64, 65, 127, 65472, math.MaxUint16}, 6},
		{"full", uint16Range(0, maxCardinality, 3), uint16Range(0, maxCardinality, 1), maxCardinality - len(uint16Range(0, maxCardinality, 3))},
	}
	impls := []struct {
		name string
		fn   func(words, vals []uint16) int
	}{
		{"chosen", orValues},
		{"mask", orValuesByMask},
	}
	for _, tc := range tcs {
		want := bitmapWords(tc.inBitmap)
		require.Equal(t, tc.wantNewCount, orValuesEach(want, tc.vals), tc.name)
		require.Equal(t, sortedUnion(tc.inBitmap, tc.vals), bitmapValues(want), tc.name)
		for _, impl := range impls {
			t.Run(tc.name+"/"+impl.name, func(t *testing.T) {
				got := bitmapWords(tc.inBitmap)
				require.Equal(t, tc.wantNewCount, impl.fn(got, tc.vals))
				require.Equal(t, want, got)
			})
		}
	}
}

func bitmapValues(words []uint16) []uint16 {
	out := make([]uint16, maxCardinality)
	return out[:bitsIntoArray(words, out, 0)]
}
