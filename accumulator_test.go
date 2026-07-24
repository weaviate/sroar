package sroar

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// refUnion computes the expected union of all sources element-wise, entirely
// independent of any sroar merge code.
func refUnion(sources []*Bitmap) []uint64 {
	seen := map[uint64]struct{}{}
	for _, s := range sources {
		for _, v := range s.ToArray() {
			seen[v] = struct{}{}
		}
	}
	out := make([]uint64, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func bitmapOf(vals ...uint64) *Bitmap {
	bm := NewBitmap()
	bm.SetMany(vals)
	return bm
}

func TestAccumulator(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	randomVals := func(n int, universe uint64) []uint64 {
		vals := make([]uint64, n)
		for i := range vals {
			vals[i] = rng.Uint64() % universe
		}
		return vals
	}

	tests := []struct {
		name    string
		sources func() []*Bitmap
	}{
		{
			name:    "no sources",
			sources: func() []*Bitmap { return nil },
		},
		{
			name:    "nil and empty sources only",
			sources: func() []*Bitmap { return []*Bitmap{nil, NewBitmap(), nil} },
		},
		{
			name: "single singleton",
			sources: func() []*Bitmap {
				return []*Bitmap{bitmapOf(123456)}
			},
		},
		{
			name: "many singletons small universe with duplicates",
			sources: func() []*Bitmap {
				sources := make([]*Bitmap, 10_000)
				for i := range sources {
					sources[i] = bitmapOf(rng.Uint64() % 300_000)
				}
				return sources
			},
		},
		{
			name: "many singletons spread universe",
			sources: func() []*Bitmap {
				sources := make([]*Bitmap, 5_000)
				for i := range sources {
					sources[i] = bitmapOf(rng.Uint64() % (1 << 40))
				}
				return sources
			},
		},
		{
			name: "small multi-element sources",
			sources: func() []*Bitmap {
				sources := make([]*Bitmap, 2_000)
				for i := range sources {
					sources[i] = bitmapOf(randomVals(1+rng.Intn(5), 1_000_000)...)
				}
				return sources
			},
		},
		{
			name: "few large sources",
			sources: func() []*Bitmap {
				sources := make([]*Bitmap, 5)
				for i := range sources {
					sources[i] = bitmapOf(randomVals(500_000, 10_000_000)...)
				}
				return sources
			},
		},
		{
			name: "mixed tiny and large",
			sources: func() []*Bitmap {
				sources := make([]*Bitmap, 0, 3_005)
				for i := 0; i < 3_000; i++ {
					sources = append(sources, bitmapOf(rng.Uint64()%10_000_000))
				}
				for i := 0; i < 5; i++ {
					sources = append(sources, bitmapOf(randomVals(200_000, 10_000_000)...))
				}
				return sources
			},
		},
		{
			name: "key zero range",
			sources: func() []*Bitmap {
				return []*Bitmap{bitmapOf(0), bitmapOf(1), bitmapOf(65535), bitmapOf(65536)}
			},
		},
		{
			name: "top key range",
			sources: func() []*Bitmap {
				// The very last 64K range and the last values in it: the
				// deposit path with lo == 0xFFFF and the largest possible key.
				return []*Bitmap{
					bitmapOf(math.MaxUint64),
					bitmapOf(math.MaxUint64 - 1),
					bitmapOf(math.MaxUint64 - 65536),
					bitmapOf(0),
				}
			},
		},
		{
			name: "array-bitmap cutoff below",
			sources: func() []*Bitmap {
				// 2048 distinct values in one 64K range: result container
				// stays an array container.
				sources := make([]*Bitmap, 2048)
				for i := range sources {
					sources[i] = bitmapOf(uint64(i) * 2)
				}
				return sources
			},
		},
		{
			name: "array-bitmap cutoff above",
			sources: func() []*Bitmap {
				// 2049 distinct values in one 64K range: result container
				// becomes a bitmap container.
				sources := make([]*Bitmap, 2049)
				for i := range sources {
					sources[i] = bitmapOf(uint64(i) * 2)
				}
				return sources
			},
		},
		{
			name: "full container",
			sources: func() []*Bitmap {
				vals := make([]uint64, 65536)
				for i := range vals {
					vals[i] = uint64(i)
				}
				return []*Bitmap{bitmapOf(vals...), bitmapOf(1, 2, 3)}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sources := tt.sources()
			want := refUnion(sources)

			acc := NewAccumulator()
			for _, s := range sources {
				acc.Or(s)
			}
			got := acc.Bitmap()

			require.Equal(t, len(want), got.GetCardinality())
			require.Equal(t, want, got.ToArray())
		})
	}
}

func TestAccumulatorReset(t *testing.T) {
	acc := NewAccumulator()
	acc.Or(bitmapOf(1, 2, 3))
	acc.Or(bitmapOf(100_000, 200_000))
	require.Equal(t, []uint64{1, 2, 3, 100_000, 200_000}, acc.Bitmap().ToArray())

	// After Reset, nothing from the previous union may leak into the next;
	// building right away yields an empty bitmap.
	acc.Reset()
	require.Equal(t, 0, acc.Bitmap().GetCardinality())
	acc.Or(bitmapOf(42))
	acc.Or(bitmapOf(300_000))
	require.Equal(t, []uint64{42, 300_000}, acc.Bitmap().ToArray())
}

func TestAccumulatorResetRetainsSpareBlocks(t *testing.T) {
	// Spares survive Reset key-independently: a following union over a
	// completely different id range must reuse them instead of allocating.
	acc := NewAccumulator()
	acc.Or(bitmapOf(42))
	spare := &acc.blocks[0][0]
	acc.Reset()
	acc.Or(bitmapOf(1 << 40))
	require.Equal(t, spare, &acc.blocks[0][0])
	require.Equal(t, []uint64{1 << 40}, acc.Bitmap().ToArray())

	// Spare retention is capped: after a union over many ranges, only
	// maxRetainedBlocks blocks may stay resident.
	acc.Reset()
	for i := 0; i < 100; i++ {
		acc.Or(bitmapOf(uint64(i) << 16))
	}
	acc.Reset()
	require.Empty(t, acc.keys)
	require.LessOrEqual(t, len(acc.free), maxRetainedBlocks)
	acc.Or(bitmapOf(5, 1<<16|7, 1<<40))
	require.Equal(t, []uint64{5, 1<<16 | 7, 1 << 40}, acc.Bitmap().ToArray())

	// The keys/blocks slices themselves are dropped once a union pushed
	// their capacity past maxRetainedSlots.
	acc.Reset()
	for i := 0; i < maxRetainedSlots+1; i++ {
		acc.Or(bitmapOf(uint64(i) << 16))
	}
	require.Equal(t, maxRetainedSlots+1, acc.Bitmap().GetCardinality())
	acc.Reset()
	require.Nil(t, acc.keys)
	acc.Or(bitmapOf(7))
	require.Equal(t, []uint64{7}, acc.Bitmap().ToArray())
}

func TestAccumulatorBitmapToBuf(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	sources := make([]*Bitmap, 20_000)
	for i := range sources {
		sources[i] = bitmapOf(rng.Uint64() % 300_000)
	}
	// Sparse singletons in high key ranges on top of the dense low range: the
	// union then holds array containers (with padding tails) alongside bitmap
	// containers, so the serialization subtests exercise both.
	for i := 0; i < 64; i++ {
		sources = append(sources, bitmapOf(uint64(i+10)<<16|uint64(i)))
	}
	want := refUnion(sources)

	build := func() *Accumulator {
		acc := NewAccumulator()
		for _, s := range sources {
			acc.Or(s)
		}
		return acc
	}

	t.Run("dirty pooled buffer", func(t *testing.T) {
		acc := build()
		var buf []byte
		got := acc.BitmapToBuf(func(n int) []byte {
			buf = make([]byte, n)
			for i := range buf {
				buf[i] = 0xFF // pooled buffers arrive dirty
			}
			return buf
		})
		require.Equal(t, want, got.ToArray())
		// The result must actually live in the obtained buffer, not on the heap.
		require.Equal(t, &buf[0], &got._ptr[0])
	})

	t.Run("too small falls back to heap", func(t *testing.T) {
		acc := build()
		got := acc.BitmapToBuf(func(int) []byte { return make([]byte, 16) })
		require.Equal(t, want, got.ToArray())
		require.Nil(t, got._ptr)
	})

	t.Run("one uint16 short falls back to heap", func(t *testing.T) {
		// The requested size fits exactly (see "result occupies exactly the
		// requested size"); one uint16 less must fall back and leave the
		// buffer untouched.
		acc := build()
		var buf []byte
		got := acc.BitmapToBuf(func(n int) []byte {
			buf = make([]byte, n-2)
			return buf
		})
		require.Equal(t, want, got.ToArray())
		require.Nil(t, got._ptr)
		require.Equal(t, make([]byte, len(buf)), buf)
	})

	t.Run("Or between builds is reflected", func(t *testing.T) {
		acc := build()
		var size, sizeGrown int
		acc.BitmapToBuf(func(n int) []byte { size = n; return make([]byte, n) })
		// Force a new range into existence: the requested size must grow.
		acc.Or(bitmapOf(1 << 40))
		got := acc.BitmapToBuf(func(n int) []byte { sizeGrown = n; return make([]byte, n) })
		require.Greater(t, sizeGrown, size)
		require.Equal(t, append(append([]uint64{}, want...), 1<<40), got.ToArray())
	})

	t.Run("result occupies exactly the requested size", func(t *testing.T) {
		// The sources touch key 0, whose pre-created container must be
		// filled in place rather than orphaned — any waste would make the
		// serialized result smaller than the buffer it was built into.
		acc := build()
		var size int
		got := acc.BitmapToBuf(func(n int) []byte { size = n; return make([]byte, n) })
		require.Len(t, got.ToBuffer(), size)
		require.Len(t, acc.Bitmap().ToBuffer(), size)
	})

	t.Run("result occupies exactly the requested size without key 0", func(t *testing.T) {
		// A union that never touches key 0 keeps the pre-created key-0
		// container as a minimum-size placeholder that is still part of the
		// serialized slab.
		acc := NewAccumulator()
		for i := 1; i <= 40; i++ {
			acc.Or(bitmapOf(uint64(i) << 16))
		}
		var size int
		got := acc.BitmapToBuf(func(n int) []byte { size = n; return make([]byte, n) })
		require.Len(t, got.ToBuffer(), size)
		require.Len(t, acc.Bitmap().ToBuffer(), size)
	})

	t.Run("dirty buffer does not leak into serialized bytes", func(t *testing.T) {
		// ToBuffer serializes the whole data slab, so every byte of the
		// result — including array-container padding — must be independent
		// of the buffer's prior contents.
		serialize := func(fill byte) []byte {
			acc := build()
			return acc.BitmapToBuf(func(n int) []byte {
				buf := make([]byte, n)
				for i := range buf {
					buf[i] = fill
				}
				return buf
			}).ToBufferWithCopy()
		}
		require.Equal(t, serialize(0x00), serialize(0xFF))
	})

	t.Run("ToBuffer FromBuffer round-trip", func(t *testing.T) {
		acc := build()
		serialized := acc.BitmapToBuf(func(n int) []byte {
			buf := make([]byte, n)
			for i := range buf {
				buf[i] = 0xFF
			}
			return buf
		}).ToBufferWithCopy()
		require.Equal(t, want, FromBuffer(serialized).ToArray())
	})

	t.Run("reset then rebuild into same buffer", func(t *testing.T) {
		acc := build()
		var buf []byte
		grab := func(n int) []byte {
			if cap(buf) < n {
				buf = make([]byte, n)
			}
			return buf[:n]
		}
		require.Equal(t, want, acc.BitmapToBuf(grab).ToArray())
		acc.Reset()
		acc.Or(bitmapOf(7, 8, 9))
		require.Equal(t, []uint64{7, 8, 9}, acc.BitmapToBuf(grab).ToArray())
	})
}

func TestAccumulatorInitBitmapToBuf(t *testing.T) {
	a := bitmapOf(1, 2, 3, 100_000)
	b := bitmapOf(42, 1<<40)

	t.Run("reuses struct and buffer across unions", func(t *testing.T) {
		// One pool entry: the struct and the buffer live together and both
		// must be reused on every build.
		var bm Bitmap
		buf := make([]byte, 4096)
		get := func(n int) (*Bitmap, []byte) {
			require.LessOrEqual(t, n, len(buf))
			return &bm, buf
		}

		acc := NewAccumulator()
		acc.Or(a)
		got := acc.InitBitmapToBuf(get)
		require.Same(t, &bm, got)
		require.Equal(t, a.ToArray(), got.ToArray())
		require.Equal(t, &buf[0], &got._ptr[0])

		acc.Reset()
		acc.Or(b)
		got = acc.InitBitmapToBuf(get)
		require.Same(t, &bm, got)
		require.Equal(t, b.ToArray(), got.ToArray())
		require.Equal(t, &buf[0], &got._ptr[0])
	})

	t.Run("too small sets the struct to a heap-built union", func(t *testing.T) {
		var bm Bitmap
		acc := NewAccumulator()
		acc.Or(a)
		got := acc.InitBitmapToBuf(func(int) (*Bitmap, []byte) {
			return &bm, make([]byte, 16)
		})
		require.Same(t, &bm, got)
		require.Equal(t, a.ToArray(), got.ToArray())
		require.Nil(t, got._ptr)
	})

	t.Run("warm accumulator with pooled memory allocates nothing", func(t *testing.T) {
		sources := []*Bitmap{bitmapOf(1, 2, 3), bitmapOf(70_000), bitmapOf(1 << 33)}
		var bm Bitmap
		buf := make([]byte, 4096)
		get := func(int) (*Bitmap, []byte) { return &bm, buf }

		acc := NewAccumulator()
		allocs := testing.AllocsPerRun(100, func() {
			acc.Reset()
			for _, s := range sources {
				acc.Or(s)
			}
			acc.InitBitmapToBuf(get)
		})
		require.Zero(t, allocs)
	})
}

func TestAccumulatorResultIsMutable(t *testing.T) {
	// The built bitmap must be a fully independent bitmap: mutating it must
	// not corrupt the builder, and building again must not affect it.
	acc := NewAccumulator()
	acc.Or(bitmapOf(1, 2, 3))
	first := acc.Bitmap()
	first.Set(99)
	require.True(t, first.Contains(99))

	second := acc.Bitmap()
	require.Equal(t, []uint64{1, 2, 3}, second.ToArray())
	require.True(t, first.Contains(99))
}
