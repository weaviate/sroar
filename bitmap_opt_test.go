package sroar

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeToSuperset(t *testing.T) {
	containerThreshold := uint64(math.MaxUint16 + 1)
	buf := make([]uint16, maxContainerSize)

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
		superset.AndToSuperset(and, buf)

		require.Equal(t, 11389, superset.GetCardinality())
		require.ElementsMatch(t, control.ToArray(), superset.ToArray())
	})

	t.Run("or", func(t *testing.T) {
		control.Or(or)
		superset.OrToSuperset(or, buf)

		require.Equal(t, 22750, superset.GetCardinality())
		require.ElementsMatch(t, control.ToArray(), superset.ToArray())
	})

	t.Run("and not", func(t *testing.T) {
		control.AndNot(andNot)
		superset.AndNotToSuperset(andNot, buf)

		require.Equal(t, 9911, superset.GetCardinality())
		require.ElementsMatch(t, control.ToArray(), superset.ToArray())
	})

	t.Run("2nd or", func(t *testing.T) {
		control.Or(or)
		superset.OrToSuperset(or, buf)

		require.Equal(t, 20730, superset.GetCardinality())
		require.ElementsMatch(t, control.ToArray(), superset.ToArray())
	})

	t.Run("2nd and", func(t *testing.T) {
		control.And(and)
		superset.AndToSuperset(and, buf)

		require.Equal(t, 10369, superset.GetCardinality())
		require.ElementsMatch(t, control.ToArray(), superset.ToArray())
	})

	t.Run("2nd and not", func(t *testing.T) {
		control.AndNot(andNot)
		superset.AndNotToSuperset(andNot, buf)

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
