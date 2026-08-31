package sroar

import (
	"fmt"
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
	{"unbounded", func() *Accumulator { return NewAccumulator().WithConc(math.MaxInt) }},
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

// accTarget is the deposit surface a fold case drives. It is implemented
// by Accumulator and by the map-based refAcc, so the same case runs once
// against each and the results are compared — which requires the case to
// be deterministic across the two runs.
type accTarget interface {
	Or(bm *Bitmap)
	AndNot(bm *Bitmap)
}

// refAcc mirrors the accumulator's fold operations over a plain map,
// entirely independent of any sroar merge code.
type refAcc struct {
	seen map[uint64]struct{}
}

func newRefAcc() *refAcc { return &refAcc{seen: map[uint64]struct{}{}} }

func (r *refAcc) Or(bm *Bitmap) {
	if bm == nil {
		return
	}
	for _, v := range bm.ToArray() {
		r.seen[v] = struct{}{}
	}
}

func (r *refAcc) AndNot(bm *Bitmap) {
	if bm == nil {
		return
	}
	for _, v := range bm.ToArray() {
		delete(r.seen, v)
	}
}

// values returns the reference's elements in ascending order.
func (r *refAcc) values() []uint64 {
	out := make([]uint64, 0, len(r.seen))
	for v := range r.seen {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// refMerge computes the expected result of merging b into a (union, or
// difference when andNot), entirely independent of any sroar merge code.
func refMerge(a, b []uint64, andNot bool) []uint64 {
	seen := map[uint64]struct{}{}
	for _, v := range a {
		seen[v] = struct{}{}
	}
	for _, v := range b {
		if andNot {
			delete(seen, v)
		} else {
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

// stage runs fn (nil for an empty side) against each target.
func stage(fn func(accTarget), targets ...accTarget) {
	if fn == nil {
		return
	}
	for _, tgt := range targets {
		fn(tgt)
	}
}

func TestAccumulatorAndNot(t *testing.T) {
	randomBitmapOf := func(rng *rand.Rand, n int, universe uint64) *Bitmap {
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

	manyRanges := func(from, to int, lo uint64) *Bitmap {
		vals := make([]uint64, 0, to-from)
		for k := from; k < to; k++ {
			vals = append(vals, uint64(k)<<16|lo)
		}
		return bitmapOf(vals...)
	}

	tests := []struct {
		name  string
		stage func(a accTarget)
	}{
		{
			name: "subtract from array-staged range",
			stage: func(a accTarget) {
				a.Or(bitmapOf(1, 2, 3, 100_000))
				a.AndNot(bitmapOf(2, 100_000))
			},
		},
		{
			name: "subtract crosses the array-bitmap cutoff",
			stage: func(a accTarget) {
				// 2049 staged values become a bitmap container; removing one
				// brings the build back under the cutoff to an array container.
				a.Or(spread2049())
				a.AndNot(bitmapOf(0))
			},
		},
		{
			name: "subtract bitmap-container subtrahend",
			stage: func(a accTarget) {
				// A dense 5000-value subtrahend is a bitmap-type container,
				// exercising the word-wise clear in andNotRange.
				or := make([]uint64, 2500)
				for i := range or {
					or[i] = uint64(i) * 3
				}
				sub := make([]uint64, 5000)
				for i := range sub {
					sub[i] = uint64(i)
				}
				a.Or(bitmapOf(or...))
				a.AndNot(bitmapOf(sub...))
			},
		},
		{
			name: "subtract untouched ranges is a no-op",
			stage: func(a accTarget) {
				a.Or(bitmapOf(5))
				a.AndNot(bitmapOf(1<<20|3, 1<<40))
			},
		},
		{
			name: "subtract everything",
			stage: func(a accTarget) {
				a.Or(bitmapOf(1, 2, 3, 1<<20, 1<<40))
				a.AndNot(bitmapOf(1, 2, 3, 1<<20, 1<<40))
			},
		},
		{
			name: "andnot before any or yields empty",
			stage: func(a accTarget) {
				a.AndNot(bitmapOf(1, 2, 3))
			},
		},
		{
			name: "nil and empty sources",
			stage: func(a accTarget) {
				a.Or(bitmapOf(7))
				a.AndNot(nil)
				a.AndNot(NewBitmap())
			},
		},
		{
			name: "re-add after subtract",
			stage: func(a accTarget) {
				a.Or(bitmapOf(7, 100_000))
				a.AndNot(bitmapOf(7, 100_000))
				a.Or(bitmapOf(7))
			},
		},
		{
			name: "top key range",
			stage: func(a accTarget) {
				a.Or(bitmapOf(math.MaxUint64, math.MaxUint64-1, 0))
				a.AndNot(bitmapOf(math.MaxUint64, 0))
			},
		},
		{
			name: "interleaved source layers",
			stage: func(a accTarget) {
				// The segment-fold shape: per layer, subtract its deletions,
				// then deposit its additions. The rng is seeded here so both
				// runs of the stage see identical layers.
				rng := rand.New(rand.NewSource(23))
				for i := 0; i < 10; i++ {
					a.AndNot(randomBitmapOf(rng, 1_000, 1_000_000))
					a.Or(randomBitmapOf(rng, 1_000, 1_000_000))
				}
			},
		},
		{
			name: "wide subtract from wide accumulator",
			stage: func(a accTarget) {
				// Hundreds of overlapping ranges on both sides: the shape
				// that engages the concurrent subtract path.
				a.Or(manyRanges(0, 300, 7))
				a.AndNot(manyRanges(50, 250, 7))
			},
		},
		{
			name: "gallop over wide source",
			stage: func(a accTarget) {
				// Narrow accumulator, >1000-range source: the walk advances
				// through the source side exponentially.
				a.Or(bitmapOf(5<<16|3, 500<<16|3, 900<<16|3, 900<<16|4))
				a.AndNot(manyRanges(0, 1200, 3))
			},
		},
		{
			name: "gallop over wide accumulator",
			stage: func(a accTarget) {
				// >1000-range accumulator, narrow source: the walk advances
				// through the accumulator side exponentially.
				a.Or(manyRanges(0, 1200, 3))
				a.AndNot(bitmapOf(5<<16|3, 900<<16|3))
			},
		},
	}

	for _, c := range accConstructors {
		t.Run(c.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					ref := newRefAcc()
					acc := c.newAcc()
					stage(tt.stage, ref, acc)
					want := ref.values()
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

func TestAccumulatorOrAcc(t *testing.T) {
	manyRanges := func(n int, lo uint64) *Bitmap {
		vals := make([]uint64, 0, n)
		for k := 0; k < n; k++ {
			vals = append(vals, uint64(k)<<16|lo)
		}
		return bitmapOf(vals...)
	}
	spread2048 := func(offset uint64) *Bitmap {
		vals := make([]uint64, 2048)
		for i := range vals {
			vals[i] = uint64(i)*2 + offset
		}
		return bitmapOf(vals...)
	}

	tests := []struct {
		name   string
		stageA func(a accTarget)
		stageB func(a accTarget)
	}{
		{
			name:   "disjoint ranges",
			stageA: func(a accTarget) { a.Or(bitmapOf(1, 2)) },
			stageB: func(a accTarget) { a.Or(bitmapOf(1<<20, 1<<40)) },
		},
		{
			name:   "overlapping ranges",
			stageA: func(a accTarget) { a.Or(bitmapOf(1, 2, 100_000)) },
			stageB: func(a accTarget) { a.Or(bitmapOf(2, 3, 100_001)) },
		},
		{
			name:   "into empty accumulator",
			stageB: func(a accTarget) { a.Or(bitmapOf(7, 1<<33)) },
		},
		{
			name:   "empty other",
			stageA: func(a accTarget) { a.Or(bitmapOf(7)) },
		},
		{
			name:   "union crosses the array-bitmap cutoff",
			stageA: func(a accTarget) { a.Or(spread2048(0)) },
			stageB: func(a accTarget) { a.Or(spread2048(1)) },
		},
		{
			name:   "emptied range in other contributes nothing",
			stageA: func(a accTarget) { a.Or(bitmapOf(1)) },
			stageB: func(a accTarget) {
				a.Or(bitmapOf(5, 1<<20))
				a.AndNot(bitmapOf(1 << 20))
			},
		},
		{
			name:   "wide both sides",
			stageA: func(a accTarget) { a.Or(manyRanges(300, 3)) },
			stageB: func(a accTarget) { a.Or(manyRanges(200, 7)) },
		},
		{
			name: "wide disjoint ranges",
			// The source introduces ranges the destination lacks, so the
			// concurrent path must pre-create every block before fan-out.
			stageA: func(a accTarget) { a.Or(bitmapOf(1)) },
			stageB: func(a accTarget) { a.Or(manyRanges(100, 9)) },
		},
		{
			name: "adopts range with upper-half bits",
			// The adopted range's only bit sits in the upper half of the
			// block, pinning the full length of the copy fast-path.
			stageA: func(a accTarget) { a.Or(bitmapOf(1)) },
			stageB: func(a accTarget) { a.Or(bitmapOf(1<<20 | 32768)) },
		},
	}

	for _, c := range accConstructors {
		t.Run(c.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					accA, refA := c.newAcc(), newRefAcc()
					accB, refB := NewAccumulator(), newRefAcc()
					stage(tt.stageA, refA, accA)
					stage(tt.stageB, refB, accB)
					want := refMerge(refA.values(), refB.values(), false)

					accA.OrAcc(accB)
					got := accA.Bitmap()
					require.Equal(t, len(want), got.GetCardinality())
					require.Equal(t, want, got.ToArray())
					// other is only read: it must stay independently usable.
					require.Equal(t, refB.values(), accB.Bitmap().ToArray())
				})
			}
		})
	}
}

func TestAccumulatorAndNotAcc(t *testing.T) {
	manyRanges := func(n int, lo uint64) *Bitmap {
		vals := make([]uint64, 0, n)
		for k := 0; k < n; k++ {
			vals = append(vals, uint64(k)<<16|lo)
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
		name   string
		stageA func(a accTarget)
		stageB func(a accTarget)
	}{
		{
			name:   "overlapping subtract",
			stageA: func(a accTarget) { a.Or(bitmapOf(1, 2, 3, 100_000)) },
			stageB: func(a accTarget) { a.Or(bitmapOf(2, 100_000, 500_000)) },
		},
		{
			name:   "disjoint is a no-op",
			stageA: func(a accTarget) { a.Or(bitmapOf(1)) },
			stageB: func(a accTarget) { a.Or(bitmapOf(1<<20, 1<<40)) },
		},
		{
			name:   "subtract everything",
			stageA: func(a accTarget) { a.Or(bitmapOf(1, 2, 3, 1<<20, 1<<40)) },
			stageB: func(a accTarget) { a.Or(bitmapOf(1, 2, 3, 1<<20, 1<<40)) },
		},
		{
			name:   "from empty accumulator",
			stageB: func(a accTarget) { a.Or(bitmapOf(1, 2)) },
		},
		{
			name:   "empty other",
			stageA: func(a accTarget) { a.Or(bitmapOf(1, 2)) },
		},
		{
			name:   "emptied range in other clears nothing",
			stageA: func(a accTarget) { a.Or(bitmapOf(1 << 20)) },
			stageB: func(a accTarget) {
				a.Or(bitmapOf(1 << 20))
				a.AndNot(bitmapOf(1 << 20))
			},
		},
		{
			name:   "subtract crosses the array-bitmap cutoff",
			stageA: func(a accTarget) { a.Or(spread2049()) },
			stageB: func(a accTarget) { a.Or(bitmapOf(0)) },
		},
		{
			name: "wide subtract from wide accumulator",
			// Both sides span enough ranges to engage the concurrent path.
			stageA: func(a accTarget) { a.Or(manyRanges(300, 7)) },
			stageB: func(a accTarget) { a.Or(manyRanges(200, 7)) },
		},
		{
			name:   "gallop over wide accumulator",
			stageA: func(a accTarget) { a.Or(manyRanges(1200, 3)) },
			stageB: func(a accTarget) { a.Or(bitmapOf(5<<16|3, 900<<16|3)) },
		},
		{
			name:   "gallop over wide other",
			stageA: func(a accTarget) { a.Or(bitmapOf(5<<16|3, 900<<16|3, 900<<16|4)) },
			stageB: func(a accTarget) { a.Or(manyRanges(1200, 3)) },
		},
	}

	for _, c := range accConstructors {
		t.Run(c.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					accA, refA := c.newAcc(), newRefAcc()
					accB, refB := NewAccumulator(), newRefAcc()
					stage(tt.stageA, refA, accA)
					stage(tt.stageB, refB, accB)
					want := refMerge(refA.values(), refB.values(), true)

					accA.AndNotAcc(accB)
					got := accA.Bitmap()
					require.Equal(t, len(want), got.GetCardinality())
					require.Equal(t, want, got.ToArray())
					// other is only read: it must stay independently usable.
					require.Equal(t, refB.values(), accB.Bitmap().ToArray())
				})
			}
		})
	}
}

func TestAccumulatorOrAccStagesEmptiedRanges(t *testing.T) {
	// OrAcc copies every staged range of other — including emptied ones —
	// mirroring how the accumulator itself retains emptied ranges.
	other := NewAccumulator()
	other.Or(bitmapOf(5, 1<<20))
	other.AndNot(bitmapOf(1 << 20))

	acc := NewAccumulator()
	acc.OrAcc(other)
	require.Len(t, acc.keys, 2)
	require.Equal(t, []uint64{5}, acc.Bitmap().ToArray())
}

func TestAccumulatorAndNotAccCreatesNoBlocks(t *testing.T) {
	// Subtracting an accumulator can never grow this one: ranges only the
	// other side staged must not get a block here.
	acc := NewAccumulator()
	acc.Or(bitmapOf(5))
	other := NewAccumulator()
	other.Or(bitmapOf(5, 1<<20, 1<<40))

	acc.AndNotAcc(other)
	require.Len(t, acc.keys, 1)
	require.Equal(t, 0, acc.Bitmap().GetCardinality())
}

func TestAccumulatorMergeAccNilOther(t *testing.T) {
	// A nil source is a no-op for both merge calls.
	acc := NewAccumulator()
	acc.Or(bitmapOf(1, 2))
	acc.OrAcc(nil)
	acc.AndNotAcc(nil)
	require.Equal(t, []uint64{1, 2}, acc.Bitmap().ToArray())
}

func TestAccumulatorMergeAccSelf(t *testing.T) {
	// OrAcc with itself is a no-op; AndNotAcc with itself empties every
	// staged range, and the accumulator stays reusable. Wide enough that
	// the conc variants engage the fan-out on the self-aliased source.
	want := make([]uint64, 0, 100)
	for k := 0; k < 100; k++ {
		want = append(want, uint64(k)<<16|9)
	}

	for _, c := range accConstructors {
		t.Run(c.name, func(t *testing.T) {
			acc := c.newAcc()
			acc.Or(bitmapOf(want...))
			acc.OrAcc(acc)
			require.Equal(t, want, acc.Bitmap().ToArray())

			acc.AndNotAcc(acc)
			require.Equal(t, 0, acc.Bitmap().GetCardinality())
			acc.Or(bitmapOf(9))
			require.Equal(t, []uint64{9}, acc.Bitmap().ToArray())
		})
	}
}

func TestAccumulatorMergeAccInterleavesWithBitmapOps(t *testing.T) {
	// Merge calls compose with bitmap deposits strictly in call order.
	part := NewAccumulator()
	part.Or(bitmapOf(10, 20))
	sub := NewAccumulator()
	sub.Or(bitmapOf(20, 99))

	acc := NewAccumulator()
	acc.Or(bitmapOf(1, 10))
	acc.OrAcc(part)          // {1, 10, 20}
	acc.AndNot(bitmapOf(10)) // {1, 20}
	acc.AndNotAcc(sub)       // {1}
	acc.Or(bitmapOf(20))     // {1, 20}
	require.Equal(t, []uint64{1, 20}, acc.Bitmap().ToArray())
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
	// concCap resolves to a concrete goroutine limit >= 1: the
	// never-configured accumulator and any value <= 1 stay serial, and
	// unbounded fan-out is requested with a large value, not a sentinel.
	tests := []struct {
		name string
		acc  func() *Accumulator
		want int
	}{
		{"zero value", func() *Accumulator { return &Accumulator{} }, 1},
		{"WithConc(0)", func() *Accumulator { return NewAccumulator().WithConc(0) }, 1},
		{"WithConc(-3)", func() *Accumulator { return NewAccumulator().WithConc(-3) }, 1},
		{"WithConc(1)", func() *Accumulator { return NewAccumulator().WithConc(1) }, 1},
		{"WithConc(8)", func() *Accumulator { return NewAccumulator().WithConc(8) }, 8},
		{"WithConc(MaxInt)", func() *Accumulator { return NewAccumulator().WithConc(math.MaxInt) }, math.MaxInt},
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
	const retain = 16

	// With retention configured, spares survive Reset key-independently: a
	// following union over a completely different id range reuses them
	// instead of allocating.
	acc := NewAccumulator().WithRetainedBlocks(retain)
	acc.Or(bitmapOf(42))
	spare := &acc.blocks[0][0]
	acc.Reset()
	acc.Or(bitmapOf(1 << 40))
	require.Equal(t, spare, &acc.blocks[0][0])
	require.Equal(t, []uint64{1 << 40}, acc.Bitmap().ToArray())

	// Spare retention is capped at the configured count: after a union over
	// many ranges, only `retain` blocks stay resident.
	acc.Reset()
	for i := 0; i < 100; i++ {
		acc.Or(bitmapOf(uint64(i) << 16))
	}
	acc.Reset()
	require.Empty(t, acc.keys)
	require.LessOrEqual(t, len(acc.free), retain)
	acc.Or(bitmapOf(5, 1<<16|7, 1<<40))
	require.Equal(t, []uint64{5, 1<<16 | 7, 1 << 40}, acc.Bitmap().ToArray())

	// The keys/blocks slices themselves are dropped once a union spread far
	// enough past the budget to push them beyond retainedSlotFactor.
	acc.Reset()
	const wide = retainedSlotFactor * retain * 4
	for i := 0; i < wide; i++ {
		acc.Or(bitmapOf(uint64(i) << 16))
	}
	require.Equal(t, wide, acc.Bitmap().GetCardinality())
	acc.Reset()
	require.Nil(t, acc.keys)
	acc.Or(bitmapOf(7))
	require.Equal(t, []uint64{7}, acc.Bitmap().ToArray())
}

func TestAccumulatorWithRetainedBlocks(t *testing.T) {
	blockPtr := func(acc *Accumulator) *uint16 { return &acc.blocks[0][0] }

	t.Run("default retains nothing", func(t *testing.T) {
		// A never-configured accumulator frees its blocks on Reset, so the
		// next union allocates fresh rather than reusing.
		acc := NewAccumulator()
		acc.Or(bitmapOf(42))
		acc.Reset()
		require.Empty(t, acc.free)
	})

	t.Run("explicit zero retains nothing", func(t *testing.T) {
		acc := NewAccumulator().WithRetainedBlocks(0)
		acc.Or(bitmapOf(42))
		acc.Reset()
		require.Empty(t, acc.free)
	})

	t.Run("negative retains nothing", func(t *testing.T) {
		acc := NewAccumulator().WithRetainedBlocks(-5)
		require.Equal(t, 0, acc.maxRetained)
	})

	t.Run("retains up to the cap and reuses", func(t *testing.T) {
		acc := NewAccumulator().WithRetainedBlocks(2)
		for i := 0; i < 5; i++ {
			acc.Or(bitmapOf(uint64(i) << 16))
		}
		acc.Reset()
		require.Len(t, acc.free, 2)

		// The retained spare backs the next union's first range.
		acc.Or(bitmapOf(1 << 40))
		require.NotEmpty(t, acc.blocks)
	})

	t.Run("survives Reset", func(t *testing.T) {
		acc := NewAccumulator().WithRetainedBlocks(4)
		acc.Or(bitmapOf(1))
		spare := blockPtr(acc)
		acc.Reset()
		require.Equal(t, 4, acc.maxRetained)
		acc.Or(bitmapOf(1 << 40))
		require.Equal(t, spare, blockPtr(acc))
	})

	t.Run("reused dirty spare is zeroed for an OR-in deposit", func(t *testing.T) {
		// Array sources OR into the block, so a reused spare must be zeroed on
		// claim (overwrite=false) — none of its old bits may survive.
		acc := NewAccumulator().WithRetainedBlocks(4)
		for i := uint64(0); i < 100; i++ {
			acc.Or(bitmapOf(i))
		}
		spare := blockPtr(acc)
		acc.Reset()
		acc.Or(bitmapOf(7))
		require.Equal(t, spare, blockPtr(acc)) // the same block is reused
		require.Equal(t, []uint64{7}, acc.Bitmap().ToArray())
	})

	t.Run("reused dirty spare is overwritten cleanly by a bitmap copy", func(t *testing.T) {
		// A dense (bitmap-container) source into a fresh block is adopted by a
		// full copy, so the spare is claimed dirty (overwrite=true, no clear);
		// the copy must still leave exactly the new bits, no stale ones.
		dense := func(base uint64) []uint64 {
			vals := make([]uint64, 3000) // > array/bitmap cutoff → bitmap container
			for i := range vals {
				vals[i] = base + uint64(i)
			}
			return vals
		}
		acc := NewAccumulator().WithRetainedBlocks(2)
		first := dense(0)
		acc.Or(bitmapOf(first...))
		spare := blockPtr(acc)
		acc.Reset()

		second := dense(10_000) // different bits, same 64K range
		acc.Or(bitmapOf(second...))
		require.Equal(t, spare, blockPtr(acc)) // the dirty spare is reused
		require.Equal(t, second, acc.Bitmap().ToArray())
	})

	t.Run("lowering the cap shrinks the free list", func(t *testing.T) {
		// The cap bounds retained memory both ways: a downward reconfigure
		// must release spares beyond the new cap on the next Reset, and 0
		// must drop them all.
		acc := NewAccumulator().WithRetainedBlocks(8)
		for i := uint64(0); i < 8; i++ {
			acc.Or(bitmapOf(i << 16)) // 8 distinct ranges
		}
		acc.Reset()
		require.Len(t, acc.free, 8)

		acc.WithRetainedBlocks(2)
		acc.Reset()
		require.Len(t, acc.free, 2)

		acc.WithRetainedBlocks(0)
		acc.Reset()
		require.Empty(t, acc.free)
	})

	t.Run("reuses a spare emptied by AndNot", func(t *testing.T) {
		// A range emptied by AndNot keeps its (now all-zero) block until
		// Reset; retained and reused for a different range, it must behave
		// like any other spare.
		acc := NewAccumulator().WithRetainedBlocks(4)
		acc.Or(bitmapOf(1, 2, 3))
		acc.AndNot(bitmapOf(1, 2, 3)) // range 0 now dirty-but-all-zero
		require.Equal(t, 0, acc.Bitmap().GetCardinality())
		spare := blockPtr(acc)
		acc.Reset()
		acc.Or(bitmapOf(1 << 40)) // different range, reuses the emptied spare
		require.Equal(t, spare, blockPtr(acc))
		require.Equal(t, []uint64{1 << 40}, acc.Bitmap().ToArray())
	})

	// A union of exactly the configured size is the natural way to use the
	// knob, and append overshoots the budget building it. The slices must
	// still be kept, or every cycle would rebuild them.
	t.Run("a union of the configured size keeps its slices and reallocates nothing", func(t *testing.T) {
		for _, ranges := range []int{17, 75, 100, 1024, 5000} {
			acc := NewAccumulator().WithRetainedBlocks(ranges)
			src := NewBitmap()
			for i := uint64(0); i < uint64(ranges); i++ {
				src.Set(i << 16)
			}
			acc.Or(src)
			acc.Bitmap()
			acc.Reset()

			require.NotNil(t, acc.keys, "ranges=%d", ranges)
			require.Len(t, acc.free, ranges, "ranges=%d", ranges)

			// Depositing and resetting reuses every block and every slot, so
			// the cycle allocates nothing. (Building the result is excluded:
			// a union over more than layoutScratchLen ranges allocates its
			// cardinalities slice regardless of retention.)
			allocs := testing.AllocsPerRun(3, func() {
				acc.Or(src)
				acc.Reset()
			})
			require.Zero(t, allocs, "ranges=%d", ranges)
		}
	})

	// Lowering the budget must hand back the arrays the wider setting grew,
	// including free's own backing array — the retained blocks move to a
	// right-sized one rather than staying reachable through the old.
	t.Run("lowering the budget releases the oversized slices", func(t *testing.T) {
		const wide = 4000
		acc := NewAccumulator().WithRetainedBlocks(wide)
		src := NewBitmap()
		for i := uint64(0); i < wide; i++ {
			src.Set(i << 16)
		}
		acc.Or(src)
		acc.Bitmap()
		acc.Reset()
		require.Len(t, acc.free, wide)
		require.Greater(t, cap(acc.free), wide/2)

		acc.WithRetainedBlocks(8)
		acc.Or(bitmapOf(1, 1<<16, 1<<32))
		acc.Bitmap()
		acc.Reset()

		require.Len(t, acc.free, 8)
		require.LessOrEqual(t, cap(acc.free), retainedSlotFactor*8)
		require.LessOrEqual(t, cap(acc.keys), retainedSlotFactor*8)
		require.LessOrEqual(t, cap(acc.blocks), retainedSlotFactor*8)

		// Still usable, and the retained spares still work.
		acc.Or(bitmapOf(7, 1<<40))
		require.Equal(t, []uint64{7, 1 << 40}, acc.Bitmap().ToArray())
	})

	// A union can drain free below a lowered budget, so its length never
	// exceeds the new one — the oversized array must still be handed back.
	t.Run("a drained free list still releases its oversized array", func(t *testing.T) {
		acc := NewAccumulator().WithRetainedBlocks(5000)
		src := NewBitmap()
		for i := uint64(0); i < 5000; i++ {
			src.Set(i << 16)
		}
		acc.Or(src)
		acc.Bitmap()
		acc.Reset()
		require.Greater(t, cap(acc.free), 5000)

		acc.WithRetainedBlocks(100)
		drain := NewBitmap()
		for i := uint64(0); i < 4950; i++ {
			drain.Set(i << 16)
		}
		acc.Or(drain) // claims all but ~50 of the spares
		require.Less(t, len(acc.free), 100)
		acc.Bitmap()
		acc.Reset()

		require.LessOrEqual(t, cap(acc.free), retainedSlotFactor*100)
	})

	// keys and blocks grow on different size classes, so one can be within
	// the bound while the other is past it; each is judged on its own.
	t.Run("keys and blocks are bounded independently", func(t *testing.T) {
		acc := NewAccumulator().WithRetainedBlocks(100)
		src := NewBitmap()
		for i := uint64(0); i < 200; i++ {
			src.Set(i << 16)
		}
		acc.Or(src)
		bound := retainedSlotFactor * 100
		require.LessOrEqual(t, cap(acc.keys), bound)
		require.Greater(t, cap(acc.blocks), bound)
		acc.Bitmap()
		acc.Reset()

		require.NotNil(t, acc.keys) // within bound, kept
		require.Nil(t, acc.blocks)  // past it, released

		acc.Or(bitmapOf(1, 1<<20))
		require.Equal(t, []uint64{1, 1 << 20}, acc.Bitmap().ToArray())
	})

	// Retention off means off: nothing is carried, including the arrays a
	// previous wide union grew.
	t.Run("retention off carries no slices", func(t *testing.T) {
		acc := NewAccumulator().WithRetainedBlocks(2000)
		src := NewBitmap()
		for i := uint64(0); i < 2000; i++ {
			src.Set(i << 16)
		}
		acc.Or(src)
		acc.Bitmap()
		acc.Reset()

		acc.WithRetainedBlocks(0)
		acc.Or(bitmapOf(5))
		acc.Bitmap()
		acc.Reset()

		require.Empty(t, acc.free)
		require.Zero(t, cap(acc.free))
		require.Nil(t, acc.keys)
		require.Nil(t, acc.blocks)
	})
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

	// Misuse names the constructor the caller reached, not the shared body
	// both of them delegate to.
	t.Run("panics name the calling constructor", func(t *testing.T) {
		require.PanicsWithValue(t, "Accumulator.BitmapToBuf: get is nil",
			func() { build().BitmapToBuf(nil) })
		require.PanicsWithValue(t, "Accumulator.InitBitmapToBuf: get is nil",
			func() { build().InitBitmapToBuf(nil) })

		acc := build()
		need := len(acc.Bitmap().ToBuffer())
		require.PanicsWithValue(t,
			fmt.Sprintf("Accumulator.BitmapToBuf: buf too small: need %d bytes, got 4", need),
			func() {
				build().BitmapToBuf(func(int) []byte { return make([]byte, 4) })
			})
		require.PanicsWithValue(t,
			fmt.Sprintf("Accumulator.InitBitmapToBuf: buf too small: need %d bytes, got 4", need),
			func() {
				build().InitBitmapToBuf(func(int) (*Bitmap, []byte) {
					return &Bitmap{}, make([]byte, 4)
				})
			})
	})

	t.Run("get using the accumulator panics", func(t *testing.T) {
		// A deposit inside get would desync the staged bits from the layout
		// snapshot the build was sized with, silently corrupting the result.
		acc := NewAccumulator()
		acc.Or(bitmapOf(100, 200, 300))
		require.PanicsWithValue(t,
			"Accumulator.BitmapToBuf: mutated during build — get must not use the accumulator",
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
			"Accumulator.BitmapToBuf: mutated during build — get must not use the accumulator",
			func() {
				acc.BitmapToBuf(func(n int) []byte {
					acc.AndNot(bitmapOf(100))
					return make([]byte, n)
				})
			})
	})

	t.Run("get merging an accumulator panics", func(t *testing.T) {
		other := NewAccumulator()
		other.Or(bitmapOf(1))
		acc := NewAccumulator()
		acc.Or(bitmapOf(100, 200, 300))
		require.PanicsWithValue(t,
			"Accumulator.BitmapToBuf: mutated during build — get must not use the accumulator",
			func() {
				acc.BitmapToBuf(func(n int) []byte {
					acc.OrAcc(other)
					return make([]byte, n)
				})
			})
	})

	t.Run("get subtracting an accumulator panics", func(t *testing.T) {
		other := NewAccumulator()
		other.Or(bitmapOf(100))
		acc := NewAccumulator()
		acc.Or(bitmapOf(100, 200, 300))
		require.PanicsWithValue(t,
			"Accumulator.BitmapToBuf: mutated during build — get must not use the accumulator",
			func() {
				acc.BitmapToBuf(func(n int) []byte {
					acc.AndNotAcc(other)
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
		other := NewAccumulator()
		other.Or(bitmapOf(5))
		var bm Bitmap
		buf := make([]byte, 4096)
		get := func(int) (*Bitmap, []byte) { return &bm, buf }

		// Reuse is opt-in: retention must be configured for a warm pooled
		// accumulator to allocate nothing across Reset.
		acc := NewAccumulator().WithRetainedBlocks(16)
		allocs := testing.AllocsPerRun(100, func() {
			acc.Reset()
			for _, s := range sources {
				acc.Or(s)
			}
			acc.AndNot(sub)
			acc.OrAcc(other)
			acc.AndNotAcc(other)
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

// linearSearchFrom is the obvious O(n) reading of searchFrom's contract:
// the smallest index in (from, to) whose key is >= k, else to.
func linearSearchFrom(keys []uint64, from, to int, k uint64) int {
	lower := from + 1
	if lower >= to {
		return lower
	}
	for i := lower; i < to; i++ {
		if keys[i] >= k {
			return i
		}
	}
	return to
}

func makeAccForSearch(numKeys int) *Accumulator {
	bm := NewBitmap()
	for i := 0; i < numKeys; i++ {
		bm.Set(uint64(i+1) << 16)
	}
	acc := NewAccumulator()
	acc.Or(bm)
	return acc
}

func TestAccumulatorSearchFrom(t *testing.T) {
	t.Run("from -1 can land on index 0", func(t *testing.T) {
		acc := makeAccForSearch(10)
		require.Equal(t, 0, acc.searchFrom(-1, len(acc.keys), acc.keys[0]))
	})

	t.Run("gap of 1 returns from+1 immediately", func(t *testing.T) {
		acc := makeAccForSearch(10)
		require.Equal(t, 1, acc.searchFrom(0, len(acc.keys), acc.keys[1]))
	})

	t.Run("exact match within range", func(t *testing.T) {
		acc := makeAccForSearch(100)
		to := len(acc.keys)
		for _, from := range []int{-1, 0, 10, 50} {
			for target := from + 1; target < to; target++ {
				got := acc.searchFrom(from, to, acc.keys[target])
				require.Equal(t, target, got, "from=%d target=%d", from, target)
			}
		}
	})

	t.Run("between two keys returns first key >= k", func(t *testing.T) {
		acc := makeAccForSearch(50)
		require.Equal(t, 6, acc.searchFrom(4, len(acc.keys), acc.keys[5]+1))
	})

	t.Run("k beyond all keys returns to", func(t *testing.T) {
		acc := makeAccForSearch(20)
		to := len(acc.keys)
		require.Equal(t, to, acc.searchFrom(0, to, acc.keys[to-1]+1))
	})

	t.Run("from+1 >= to returns from+1", func(t *testing.T) {
		acc := makeAccForSearch(5)
		to := len(acc.keys)
		require.Equal(t, to, acc.searchFrom(to-1, to, acc.keys[to-1]+1))
	})

	t.Run("large gap exercises the exponential path", func(t *testing.T) {
		acc := makeAccForSearch(1000)
		to := len(acc.keys)
		require.Equal(t, to-1, acc.searchFrom(-1, to, acc.keys[to-1]))
		require.Equal(t, 901, acc.searchFrom(0, to, acc.keys[900]+1))
	})

	// to is a partition bound, not just the slice length: the concurrent
	// paths hand each worker a sub-range and the walk must stop at it.
	t.Run("stops at to rather than the end of keys", func(t *testing.T) {
		acc := makeAccForSearch(200)
		for _, to := range []int{1, 2, 37, 100, 199, 200} {
			for _, from := range []int{-1, 0, 5, 50} {
				if from >= to {
					continue
				}
				beyond := acc.keys[len(acc.keys)-1] + 1
				require.Equal(t, to, acc.searchFrom(from, to, beyond),
					"from=%d to=%d", from, to)
				// A key that exists past to must not be found.
				if to < len(acc.keys) {
					require.Equal(t, to, acc.searchFrom(from, to, acc.keys[to]),
						"from=%d to=%d", from, to)
				}
			}
		}
	})

	t.Run("agrees with a linear walk across positions, targets and bounds", func(t *testing.T) {
		acc := makeAccForSearch(200)
		keys := acc.keys
		for _, to := range []int{1, 17, 128, len(keys)} {
			for from := -1; from < to; from++ {
				for _, offset := range []int{1, 2, 8, 16, 64, 128} {
					target := from + offset
					if target >= len(keys) {
						break
					}
					for _, k := range []uint64{keys[target], keys[target] + 1, keys[target] - 1} {
						require.Equal(t, linearSearchFrom(keys, from, to, k),
							acc.searchFrom(from, to, k),
							"from=%d to=%d k=%d", from, to, k)
					}
				}
			}
		}
	})
}
