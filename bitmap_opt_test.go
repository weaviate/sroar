package sroar

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeToSuperset(t *testing.T) {
	run := func(t *testing.T, bufs [][]uint16) {
		containerThreshold := uint64(math.MaxUint16 + 1)

		// containers of type array + bitmap + bitmap
		superset := NewBitmap()
		// containers of type array + array + bitmap
		and := NewBitmap()
		or := NewBitmap()
		andNot := NewBitmap()

		t.Run("init bitmaps", func(t *testing.T) {
			N1 := uint64(4000)  // fits to array container
			N2 := uint64(16000) // fits to bitmap container

			// containers of type array for all BMs
			for i := uint64(0); i < N1; i++ {
				val1 := i * 2

				superset.Set(val1)
				if i%3 != 0 {
					and.Set(i)
				}
				if i < N1*3/4 {
					or.Set(i)
				}
				if i%2 == 0 {
					andNot.Set(i)
				}
			}

			// containers of type 2xbitmap for superset
			// containers of type array+bitmap for subsets
			for i := uint64(0); i < N2; i++ {
				val2 := i*3 + containerThreshold
				val3 := i*4 + 2*containerThreshold

				superset.Set(val2)
				superset.Set(val3)

				if i%5 == 1 {
					and.Set(val2)
				}
				if a := i % 11; a == 3 || a == 7 {
					or.Set(val2)
				}
				if a := i % 23; a < 5 {
					andNot.Set(val2)
				}

				if a := i % 7; a > 3 {
					and.Set(val3)
				}
				if a := i % 13; a < 10 {
					or.Set(val3)
				}
				if a := i % 17; a > 2 && a < 15 {
					andNot.Set(val3)
				}
			}
		})

		control := superset.Clone()

		t.Run("and", func(t *testing.T) {
			control.And(and)
			superset.AndToSuperset(and, bufs...)

			require.Equal(t, 11389, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("or", func(t *testing.T) {
			control.Or(or)
			superset.OrToSuperset(or, bufs...)

			require.Equal(t, 22750, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("and not", func(t *testing.T) {
			control.AndNot(andNot)
			superset.AndNotToSuperset(andNot, bufs...)

			require.Equal(t, 9911, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("2nd or", func(t *testing.T) {
			control.Or(or)
			superset.OrToSuperset(or, bufs...)

			require.Equal(t, 20730, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("2nd and", func(t *testing.T) {
			control.And(and)
			superset.AndToSuperset(and, bufs...)

			require.Equal(t, 10369, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("2nd and not", func(t *testing.T) {
			control.AndNot(andNot)
			superset.AndNotToSuperset(andNot, bufs...)

			require.Equal(t, 5520, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("merge into", func(t *testing.T) {
			dst := NewBitmap()
			for _, val1 := range []uint64{0123, 1234, 2345, 3456, 4567, 5678, 6789, 7890, 8901, 9012} {
				val2 := val1 + containerThreshold
				val3 := val1 + 2*containerThreshold

				superset.Set(val1)
				superset.Set(val2)
				superset.Set(val3)
				control.Set(val1)
				control.Set(val2)
				control.Set(val3)

				dst.Set(val1)
				dst.Set(val2)
				dst.Set(val3)
			}
			controlDst := dst.Clone()

			require.Equal(t, 5548, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())

			dst.And(superset)
			controlDst.And(control)

			require.Equal(t, 30, dst.GetCardinality())
			require.ElementsMatch(t, controlDst.ToArray(), dst.ToArray())

			dst.Or(superset)
			controlDst.Or(control)

			require.Equal(t, 5548, dst.GetCardinality())
			require.ElementsMatch(t, controlDst.ToArray(), dst.ToArray())
		})
	}

	t.Run("single buffer", func(t *testing.T) {
		run(t, makeContainerBuffers(1))
	})

	t.Run("multiple buffers (concurrent)", func(t *testing.T) {
		run(t, makeContainerBuffers(4))
	})
}

// go test -v -fuzz FuzzMergeToSuperset -fuzztime 600s -run ^$ github.com/weaviate/sroar
func FuzzMergeToSuperset(f *testing.F) {
	type testCase struct {
		name          string
		countElements int
		countSubsets  int
		countMerges   int
		countBuffers  int
		randSeed      int64
	}

	testCases := []testCase{
		{
			name:          "few elements, few subsets",
			countElements: 1_000,
			countSubsets:  3,
			countMerges:   15,
			countBuffers:  1,
			randSeed:      1724861525311406000,
		},
		{
			name:          "few elements, many subsets",
			countElements: 2_000,
			countSubsets:  15,
			countMerges:   14,
			countBuffers:  2,
			randSeed:      172486152531140600,
		},
		{
			name:          "more elements, few subsets",
			countElements: 5_000,
			countSubsets:  4,
			countMerges:   13,
			countBuffers:  3,
			randSeed:      17248615253114060,
		},
		{
			name:          "more elements, many subsets",
			countElements: 7_000,
			countSubsets:  16,
			countMerges:   12,
			countBuffers:  4,
			randSeed:      1724861525311406,
		},
		{
			name:          "many elements, few subsets",
			countElements: 19_000,
			countSubsets:  5,
			countMerges:   11,
			countBuffers:  5,
			randSeed:      172486152531140,
		},
		{
			name:          "many elements, many subsets",
			countElements: 25_000,
			countSubsets:  18,
			countMerges:   10,
			countBuffers:  6,
			randSeed:      17248615253114,
		},
	}

	for _, tc := range testCases {
		f.Add(tc.countElements, tc.countSubsets, tc.countMerges, tc.countBuffers, tc.randSeed)
	}

	f.Fuzz(runMergeToSuperSetTest)
}

func TestMergeToSuperset_VerifyFuzzCallback(t *testing.T) {
	t.Run("single buffer", func(t *testing.T) {
		runMergeToSuperSetTest(t, 23_456, 17, 9, 1, 1724861525311)
	})

	t.Run("multiple buffers (concurrent)", func(t *testing.T) {
		runMergeToSuperSetTest(t, 23_456, 17, 9, 4, 1724861525311)
	})
}

func runMergeToSuperSetTest(t *testing.T,
	countElements, countSubsets, countMerges, countBuffers int, randSeed int64,
) {
	if countElements < 100 || countElements > 50_000 {
		return
	}
	if countSubsets < 1 || countSubsets > 25 {
		return
	}
	if countMerges < 1 || countMerges > 50 {
		return
	}
	if countBuffers < 1 || countBuffers > 32 {
		return
	}

	// max element is 3x bigger than capacity of single bm's container
	maxX := (int(math.MaxUint16) + 1) * 3
	buffers := makeContainerBuffers(countBuffers)
	rnd := rand.New(rand.NewSource(randSeed))

	superset := NewBitmap()
	subsets := make([]*Bitmap, countSubsets)
	var control *Bitmap

	t.Run("populate bitmaps", func(t *testing.T) {
		for i := 0; i < countElements; i++ {
			x := uint64(rnd.Intn(maxX))
			superset.Set(x)
		}

		for i := range subsets {
			subsets[i] = NewBitmap()
			// each next subset bitmap contains fewer elements
			// 1/2 of countElements, 1/3, 1/4, ...
			for j, c := 0, countElements/(i+2); j < c; j++ {
				x := uint64(rnd.Intn(maxX))
				subsets[i].Set(x)
				// ensure superset contains element of subset
				superset.Set(x)
			}
		}

		control = superset.Clone()
	})

	for i := 0; i < countMerges; i++ {
		t.Run("merge bitmaps", func(t *testing.T) {
			id := rnd.Intn(len(subsets))
			subset := subsets[id]

			switch mergeType := rnd.Intn(3); mergeType {
			case 1:
				t.Run(fmt.Sprintf("AND with %d", id), func(t *testing.T) {
					superset.AndToSuperset(subset, buffers...)
					control.And(subset)
					assertMatches(t, superset, control)
				})
			case 2:
				t.Run(fmt.Sprintf("AND NOT with %d", id), func(t *testing.T) {
					superset.AndNotToSuperset(subset, buffers...)
					control.AndNot(subset)
					assertMatches(t, superset, control)
				})
			default:
				t.Run(fmt.Sprintf("OR with %d", id), func(t *testing.T) {
					superset.OrToSuperset(subset, buffers...)
					control.Or(subset)
					assertMatches(t, superset, control)
				})
			}
		})
	}
}

func assertMatches(t *testing.T, bm1, bm2 *Bitmap) {
	require.Equal(t, bm1.GetCardinality(), bm2.GetCardinality())

	// check elements match using iterator as
	// require.ElementsMatch(t, bm1.ToArray(), bm2.ToArray())
	// causes fuzz test to fail frequently
	cit := bm1.NewIterator()
	sit := bm2.NewIterator()
	for {
		cx := cit.Next()
		sx := sit.Next()
		require.Equal(t, cx, sx)

		if cx == 0 || sx == 0 {
			break
		}
	}
}

func makeContainerBuffers(n int) [][]uint16 {
	bufs := make([][]uint16, n)
	for i := range bufs {
		bufs[i] = make([]uint16, maxContainerSize)
	}
	return bufs
}
