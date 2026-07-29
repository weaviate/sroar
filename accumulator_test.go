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

// accConstructors runs the Or/AndNot tables under the serial, capped, and
// uncapped accumulator variants.
var accConstructors = []struct {
	name   string
	newAcc func() *Accumulator
}{
	{"serial", NewAccumulator},
	{"conc", func() *Accumulator { return NewAccumulator().WithConc(4) }},
	{"unbounded", func() *Accumulator { return NewAccumulator().WithConc(0) }},
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
		{
			name: "wide source spanning many ranges",
			sources: func() []*Bitmap {
				// A single source with hundreds of containers: the shape
				// that engages the concurrent deposit path.
				vals := make([]uint64, 0, 200*100)
				for k := 0; k < 200; k++ {
					for j := 0; j < 100; j++ {
						vals = append(vals, uint64(k)<<16|uint64(j*13))
					}
				}
				return []*Bitmap{bitmapOf(vals...), bitmapOf(42)}
			},
		},
	}

	for _, c := range accConstructors {
		t.Run(c.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					sources := tt.sources()
					want := refUnion(sources)

					acc := c.newAcc()
					for _, s := range sources {
						acc.Or(s)
					}
					got := acc.Bitmap()

					require.Equal(t, len(want), got.GetCardinality())
					require.Equal(t, want, got.ToArray())
				})
			}
		})
	}
}

func TestAccumulatorOrSkipsEmptyContainers(t *testing.T) {
	// Removing every element of a container leaves a zero-cardinality
	// container inside a non-empty bitmap; Or must not stage its range. A
	// source whose containers were ALL emptied spans enough ranges to
	// engage the concurrent path, yet must stage nothing.
	tests := []struct {
		name     string
		bm       func() *Bitmap
		wantKeys int
		want     []uint64
	}{
		{
			name: "one emptied container",
			bm: func() *Bitmap {
				bm := bitmapOf(5, 1<<20)
				bm.Remove(5)
				return bm
			},
			wantKeys: 1,
			want:     []uint64{1 << 20},
		},
		{
			name: "all containers emptied wide source",
			bm: func() *Bitmap {
				vals := make([]uint64, 0, 100)
				for k := 0; k < 100; k++ {
					vals = append(vals, uint64(k)<<16|5)
				}
				bm := bitmapOf(vals...)
				for _, v := range vals {
					bm.Remove(v)
				}
				return bm
			},
			wantKeys: 0,
			want:     []uint64{},
		},
	}

	for _, c := range accConstructors {
		t.Run(c.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					acc := c.newAcc()
					acc.Or(tt.bm())
					require.Len(t, acc.keys, tt.wantKeys)
					got := acc.Bitmap()
					require.Equal(t, len(tt.want), got.GetCardinality())
					require.Equal(t, tt.want, got.ToArray())
				})
			}
		})
	}
}

func TestAccumulatorZeroValue(t *testing.T) {
	var acc Accumulator
	acc.Or(bitmapOf(1, 100_000))
	acc.Or(bitmapOf(2))
	require.Equal(t, []uint64{1, 2, 100_000}, acc.Bitmap().ToArray())
}

func TestAccumulatorOrUnknownContainerType(t *testing.T) {
	// FromBuffer adopts arbitrary bytes without validating container type
	// tags; a tag that is neither array nor bitmap must fail loudly instead
	// of silently dropping the container's elements from the union.
	bm := bitmapOf(5)
	bm.getContainer(bm.keys.val(0))[indexType] = typeBitmap + 1

	acc := NewAccumulator()
	require.Panics(t, func() { acc.Or(bm) })
}

// accOp is one step of an Or/AndNot fold.
type accOp struct {
	andNot bool
	bm     *Bitmap
}

// refFold computes the expected result of applying ops in order, entirely
// independent of any sroar merge code.
func refFold(ops []accOp) []uint64 {
	seen := map[uint64]struct{}{}
	for _, op := range ops {
		if op.bm == nil {
			continue
		}
		for _, v := range op.bm.ToArray() {
			if op.andNot {
				delete(seen, v)
			} else {
				seen[v] = struct{}{}
			}
		}
	}
	out := make([]uint64, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func TestAccumulatorAndNot(t *testing.T) {
	rng := rand.New(rand.NewSource(23))

	randomBitmapOf := func(n int, universe uint64) *Bitmap {
		vals := make([]uint64, n)
		for i := range vals {
			vals[i] = rng.Uint64() % universe
		}
		return bitmapOf(vals...)
	}

	spread2049 := func() *Bitmap {
		vals := make([]uint64, 2049)
		for i := range vals {
			vals[i] = uint64(i) * 2
		}
		return bitmapOf(vals...)
	}

	tests := []struct {
		name string
		ops  func() []accOp
	}{
		{
			name: "subtract from array-staged range",
			ops: func() []accOp {
				return []accOp{
					{bm: bitmapOf(1, 2, 3, 100_000)},
					{andNot: true, bm: bitmapOf(2, 100_000)},
				}
			},
		},
		{
			name: "subtract crosses the array-bitmap cutoff",
			ops: func() []accOp {
				// 2049 staged values become a bitmap container; removing one
				// brings the build back under the cutoff to an array container.
				return []accOp{
					{bm: spread2049()},
					{andNot: true, bm: bitmapOf(0)},
				}
			},
		},
		{
			name: "subtract bitmap-container subtrahend",
			ops: func() []accOp {
				// A dense 5000-value subtrahend is a bitmap-type container,
				// exercising the word-wise clear in depositAndNot.
				or := make([]uint64, 2500)
				for i := range or {
					or[i] = uint64(i) * 3
				}
				sub := make([]uint64, 5000)
				for i := range sub {
					sub[i] = uint64(i)
				}
				return []accOp{{bm: bitmapOf(or...)}, {andNot: true, bm: bitmapOf(sub...)}}
			},
		},
		{
			name: "subtract untouched ranges is a no-op",
			ops: func() []accOp {
				return []accOp{
					{bm: bitmapOf(5)},
					{andNot: true, bm: bitmapOf(1<<20|3, 1<<40)},
				}
			},
		},
		{
			name: "subtract everything",
			ops: func() []accOp {
				return []accOp{
					{bm: bitmapOf(1, 2, 3, 1<<20, 1<<40)},
					{andNot: true, bm: bitmapOf(1, 2, 3, 1<<20, 1<<40)},
				}
			},
		},
		{
			name: "andnot before any or yields empty",
			ops: func() []accOp {
				return []accOp{{andNot: true, bm: bitmapOf(1, 2, 3)}}
			},
		},
		{
			name: "nil and empty sources",
			ops: func() []accOp {
				return []accOp{
					{bm: bitmapOf(7)},
					{andNot: true, bm: nil},
					{andNot: true, bm: NewBitmap()},
				}
			},
		},
		{
			name: "re-add after subtract",
			ops: func() []accOp {
				return []accOp{
					{bm: bitmapOf(7, 100_000)},
					{andNot: true, bm: bitmapOf(7, 100_000)},
					{bm: bitmapOf(7)},
				}
			},
		},
		{
			name: "top key range",
			ops: func() []accOp {
				return []accOp{
					{bm: bitmapOf(math.MaxUint64, math.MaxUint64-1, 0)},
					{andNot: true, bm: bitmapOf(math.MaxUint64, 0)},
				}
			},
		},
		{
			name: "interleaved source layers",
			ops: func() []accOp {
				// The segment-fold shape: per layer, subtract its deletions,
				// then deposit its additions.
				ops := make([]accOp, 0, 20)
				for i := 0; i < 10; i++ {
					ops = append(ops,
						accOp{andNot: true, bm: randomBitmapOf(1_000, 1_000_000)},
						accOp{bm: randomBitmapOf(1_000, 1_000_000)},
					)
				}
				return ops
			},
		},
		{
			name: "wide subtract from wide accumulator",
			ops: func() []accOp {
				// Hundreds of overlapping ranges on both sides: the shape
				// that engages the concurrent subtract path.
				or := make([]uint64, 0, 300)
				sub := make([]uint64, 0, 200)
				for k := 0; k < 300; k++ {
					or = append(or, uint64(k)<<16|7)
				}
				for k := 50; k < 250; k++ {
					sub = append(sub, uint64(k)<<16|7)
				}
				return []accOp{{bm: bitmapOf(or...)}, {andNot: true, bm: bitmapOf(sub...)}}
			},
		},
		{
			name: "gallop over wide source",
			ops: func() []accOp {
				// Narrow accumulator, >1000-range source: the walk advances
				// through the source side exponentially.
				sub := make([]uint64, 0, 1200)
				for k := 0; k < 1200; k++ {
					sub = append(sub, uint64(k)<<16|3)
				}
				return []accOp{
					{bm: bitmapOf(5<<16|3, 500<<16|3, 900<<16|3, 900<<16|4)},
					{andNot: true, bm: bitmapOf(sub...)},
				}
			},
		},
		{
			name: "gallop over wide accumulator",
			ops: func() []accOp {
				// >1000-range accumulator, narrow source: the walk advances
				// through the accumulator side exponentially.
				or := make([]uint64, 0, 1200)
				for k := 0; k < 1200; k++ {
					or = append(or, uint64(k)<<16|3)
				}
				return []accOp{
					{bm: bitmapOf(or...)},
					{andNot: true, bm: bitmapOf(5<<16|3, 900<<16|3)},
				}
			},
		},
	}

	for _, c := range accConstructors {
		t.Run(c.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					ops := tt.ops()
					want := refFold(ops)

					acc := c.newAcc()
					for _, op := range ops {
						if op.andNot {
							acc.AndNot(op.bm)
						} else {
							acc.Or(op.bm)
						}
					}
					got := acc.Bitmap()

					require.Equal(t, len(want), got.GetCardinality())
					require.Equal(t, want, got.ToArray())
				})
			}
		})
	}
}

func TestAccumulatorAndNotEmptiedContainerClearsNothing(t *testing.T) {
	// Removing every element of a subtrahend container leaves a
	// zero-cardinality container; the staged bits of that range must
	// survive the subtraction untouched.
	sub := bitmapOf(5, 1<<20)
	sub.Remove(1 << 20)

	acc := NewAccumulator()
	acc.Or(bitmapOf(5, 1<<20))
	acc.AndNot(sub)
	require.Equal(t, []uint64{1 << 20}, acc.Bitmap().ToArray())
}

func TestAccumulatorAndNotSerializationRoundTrip(t *testing.T) {
	// A result shaped by AndNot — an emptied key-0 container and a fully
	// vanished range — must serialize into bytes FromBuffer reads back
	// identically, even from a dirty pooled buffer.
	acc := NewAccumulator()
	acc.Or(bitmapOf(0, 1, 2, 1<<20, 1<<20|1, 1<<40))
	acc.AndNot(bitmapOf(0, 1, 2, 1<<40))
	want := []uint64{1 << 20, 1<<20 | 1}

	serialized := acc.BitmapToBuf(func(n int) []byte {
		buf := make([]byte, n)
		for i := range buf {
			buf[i] = 0xFF
		}
		return buf
	}).ToBufferWithCopy()
	require.Equal(t, want, FromBuffer(serialized).ToArray())
}

func TestAccumulatorAndNotUnknownContainerType(t *testing.T) {
	// Same contract as Or: a container type tag that is neither array nor
	// bitmap must fail loudly instead of silently leaving elements behind.
	bm := bitmapOf(5)
	bm.getContainer(bm.keys.val(0))[indexType] = typeBitmap + 1

	acc := NewAccumulator()
	acc.Or(bitmapOf(5))
	require.Panics(t, func() { acc.AndNot(bm) })
}

func TestAccumulatorAndNotCreatesNoBlocks(t *testing.T) {
	// Subtracting can never grow the accumulator: ranges it never touched
	// must not get a staging block allocated just to clear bits in it.
	acc := NewAccumulator()
	acc.Or(bitmapOf(5))
	require.Len(t, acc.keys, 1)
	acc.AndNot(bitmapOf(5, 1<<20, 1<<40))
	require.Len(t, acc.keys, 1)
	require.Equal(t, 0, acc.Bitmap().GetCardinality())
}

func TestAccumulatorWithConcRetuning(t *testing.T) {
	// A pooled accumulator is retuned between uses: WithConc may be called
	// between calls and the cap survives Reset.
	wide := func() *Bitmap {
		vals := make([]uint64, 0, 100)
		for k := 0; k < 100; k++ {
			vals = append(vals, uint64(k)<<16|9)
		}
		return bitmapOf(vals...)
	}

	acc := NewAccumulator()
	acc.Or(wide())
	require.Equal(t, 100, acc.Bitmap().GetCardinality())

	acc.Reset()
	acc.WithConc(4)
	acc.Or(wide())
	acc.AndNot(wide())
	require.Equal(t, 0, acc.Bitmap().GetCardinality())

	acc.Reset()
	require.Equal(t, 4, acc.maxConc)
}

func TestAccumulatorConcCap(t *testing.T) {
	// calcConcurrency reads a cap of 0 as "no limit", so the translation
	// must keep the never-configured accumulator serial while explicit
	// WithConc(<=0) lifts the cap.
	tests := []struct {
		name string
		acc  func() *Accumulator
		want int
	}{
		{"zero value", func() *Accumulator { return &Accumulator{} }, 1},
		{"WithConc(0)", func() *Accumulator { return NewAccumulator().WithConc(0) }, 0},
		{"WithConc(-3)", func() *Accumulator { return NewAccumulator().WithConc(-3) }, 0},
		{"WithConc(1)", func() *Accumulator { return NewAccumulator().WithConc(1) }, 1},
		{"WithConc(8)", func() *Accumulator { return NewAccumulator().WithConc(8) }, 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.acc().concCap())
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

	t.Run("too small panics", func(t *testing.T) {
		acc := build()
		require.Panics(t, func() {
			acc.BitmapToBuf(func(int) []byte { return make([]byte, 16) })
		})
	})

	t.Run("one uint16 short panics leaving the buffer untouched", func(t *testing.T) {
		// The requested size fits exactly (see "result occupies exactly the
		// requested size"); one uint16 less must panic without writing to
		// the buffer.
		acc := build()
		var buf []byte
		require.Panics(t, func() {
			acc.BitmapToBuf(func(n int) []byte {
				buf = make([]byte, n-2)
				return buf
			})
		})
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
		// serialized bytes.
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
		// ToBuffer serializes the whole data array, so every byte of the
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

	t.Run("dirty buffer does not leak without key 0", func(t *testing.T) {
		// The key-0 placeholder is the one container the build never
		// overwrites — its up-front clearing alone must keep serialization
		// deterministic.
		serialize := func(fill byte) []byte {
			acc := NewAccumulator()
			for i := 1; i <= 40; i++ {
				acc.Or(bitmapOf(uint64(i)<<16 | uint64(i)))
			}
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

	t.Run("odd-capacity buffer", func(t *testing.T) {
		acc := build()
		var buf []byte
		got := acc.BitmapToBuf(func(n int) []byte {
			buf = make([]byte, n, n+1) // odd spare byte the bitmap cannot use
			return buf
		})
		require.Equal(t, want, got.ToArray())
		require.Equal(t, &buf[0], &got._ptr[0])
	})

	t.Run("nil buffer panics", func(t *testing.T) {
		acc := build()
		require.Panics(t, func() {
			acc.BitmapToBuf(func(int) []byte { return nil })
		})
	})

	t.Run("get using the accumulator panics", func(t *testing.T) {
		// A deposit inside get would desync the staged bits from the layout
		// snapshot the build was sized with, silently corrupting the result.
		acc := NewAccumulator()
		acc.Or(bitmapOf(100, 200, 300))
		require.PanicsWithValue(t,
			"Accumulator: mutated during build — get must not use the accumulator",
			func() {
				acc.BitmapToBuf(func(n int) []byte {
					acc.Or(bitmapOf(1, 2, 3))
					return make([]byte, n)
				})
			})
	})

	t.Run("get subtracting from the accumulator panics", func(t *testing.T) {
		acc := NewAccumulator()
		acc.Or(bitmapOf(100, 200, 300))
		require.PanicsWithValue(t,
			"Accumulator: mutated during build — get must not use the accumulator",
			func() {
				acc.BitmapToBuf(func(n int) []byte {
					acc.AndNot(bitmapOf(100))
					return make([]byte, n)
				})
			})
	})

	t.Run("get resetting the accumulator panics", func(t *testing.T) {
		acc := NewAccumulator()
		acc.Or(bitmapOf(100, 200, 300))
		require.Panics(t, func() {
			acc.BitmapToBuf(func(n int) []byte {
				acc.Reset()
				return make([]byte, n)
			})
		})
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

	t.Run("too small panics", func(t *testing.T) {
		var bm Bitmap
		acc := NewAccumulator()
		acc.Or(a)
		require.Panics(t, func() {
			acc.InitBitmapToBuf(func(int) (*Bitmap, []byte) {
				return &bm, make([]byte, 16)
			})
		})
	})

	t.Run("dirty buffer serializes deterministically at exact size", func(t *testing.T) {
		// Same guarantees the BitmapToBuf subtests assert, routed directly
		// through the Init path: byte-deterministic serialization filling
		// the buffer exactly. The union holds array containers, so padding
		// tails are exercised.
		var size int
		serialize := func(fill byte) []byte {
			var bm Bitmap
			acc := NewAccumulator()
			acc.Or(a)
			return acc.InitBitmapToBuf(func(n int) (*Bitmap, []byte) {
				size = n
				buf := make([]byte, n)
				for i := range buf {
					buf[i] = fill
				}
				return &bm, buf
			}).ToBufferWithCopy()
		}
		s0, s1 := serialize(0x00), serialize(0xFF)
		require.Equal(t, s0, s1)
		require.Len(t, s0, size)
	})

	t.Run("warm accumulator with pooled memory allocates nothing", func(t *testing.T) {
		sources := []*Bitmap{bitmapOf(1, 2, 3), bitmapOf(70_000), bitmapOf(1 << 33)}
		sub := bitmapOf(2, 70_000)
		var bm Bitmap
		buf := make([]byte, 4096)
		get := func(int) (*Bitmap, []byte) { return &bm, buf }

		acc := NewAccumulator()
		allocs := testing.AllocsPerRun(100, func() {
			acc.Reset()
			for _, s := range sources {
				acc.Or(s)
			}
			acc.AndNot(sub)
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
