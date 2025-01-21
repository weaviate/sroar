package sroar

import (
	"fmt"
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
