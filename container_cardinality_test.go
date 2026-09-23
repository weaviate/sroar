package sroar

import (
	"math/bits"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// cardinalityNarrow is the oracle bitmap.cardinality is checked against.
func cardinalityNarrow(b bitmap) int {
	var num int
	for _, x := range b[startIdx:] {
		num += bits.OnesCount16(x)
	}
	return num
}

func TestBitmapCardinalityWidthAgrees(t *testing.T) {
	newContainer := func(fill func(words []uint64)) bitmap {
		c := make([]uint16, maxContainerSize)
		c[indexSize] = uint16(maxContainerSize)
		c[indexType] = typeBitmap
		fill(uint16To64SliceUnsafe(c[startIdx:]))
		return bitmap(c)
	}

	rnd := rand.New(rand.NewSource(5))
	cases := []struct {
		name string
		fill func(words []uint64)
	}{
		{"all zero", func(words []uint64) {}},
		{"all ones", func(words []uint64) {
			for i := range words {
				words[i] = ^uint64(0)
			}
		}},
		{"single bit in first word", func(words []uint64) { words[0] = 1 }},
		{"single bit in last word", func(words []uint64) { words[len(words)-1] = 1 << 63 }},
		{"alternating words", func(words []uint64) {
			for i := range words {
				if i%2 == 0 {
					words[i] = ^uint64(0)
				}
			}
		}},
		{"alternating bits", func(words []uint64) {
			for i := range words {
				words[i] = 0xAAAAAAAAAAAAAAAA
			}
		}},
		{"random dense", func(words []uint64) {
			for i := range words {
				words[i] = ^(rnd.Uint64() & rnd.Uint64())
			}
		}},
		{"random sparse", func(words []uint64) {
			for i := range words {
				words[i] = rnd.Uint64() & rnd.Uint64() & rnd.Uint64() & rnd.Uint64()
			}
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := newContainer(c.fill)
			require.Equal(t, cardinalityNarrow(b), b.cardinality())
		})
	}

	t.Run("full container reports maxCardinality", func(t *testing.T) {
		b := newContainer(func(words []uint64) {
			for i := range words {
				words[i] = ^uint64(0)
			}
		})
		require.Equal(t, maxCardinality, b.cardinality())
	})

	// Pins the narrow-loop fallback for slice lengths not a multiple of 4.
	t.Run("data length not a multiple of four", func(t *testing.T) {
		c := make([]uint16, int(startIdx)+6)
		c[indexType] = typeBitmap
		c[indexSize] = uint16(len(c))
		c[startIdx] = 0xFFFF
		c[startIdx+3] = 0x00FF
		c[startIdx+5] = 0x0001
		require.Equal(t, 16+8+1, bitmap(c).cardinality())
		require.Equal(t, cardinalityNarrow(bitmap(c)), bitmap(c).cardinality())
	})
}
