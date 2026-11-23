package sroar

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// bufPoolFunc adapts a func to BufPool.
type bufPoolFunc func(minBytes int) ([]byte, func())

func (f bufPoolFunc) Get(minBytes int) ([]byte, func()) { return f(minBytes) }

// heapPool hands out fresh memory of the requested size.
var heapPool = getPool(func(minBytes int) []byte { return make([]byte, minBytes) })

// getPool adapts a buffer-only func to BufPool, with nil releases.
func getPool(get func(minBytes int) []byte) BufPool {
	return bufPoolFunc(func(minBytes int) ([]byte, func()) { return get(minBytes), nil })
}

// tailOnly builds through pool, leaving its surplus as tail room.
func tailOnly(pool BufPool) BuildOpts { return BuildOpts{Pool: pool, SpareTailOnly: true} }

// bufPoolOf hands out buf once, whatever the size asked, and heap memory after.
func bufPoolOf(buf []byte) BufPool {
	used := false
	return getPool(func(minBytes int) []byte {
		if used {
			return make([]byte, minBytes)
		}
		used = true
		return buf
	})
}

func randomBitmap(seed int64, n int, universe uint64) *Bitmap {
	rng := rand.New(rand.NewSource(seed))
	bm := NewBitmap()
	for i := 0; i < n; i++ {
		bm.Set(rng.Uint64() % universe)
	}
	return bm
}

func TestFromBufferOwnedGrowsIntoSpareCapacity(t *testing.T) {
	serialized := randomBitmap(1, 10_000, 1_000_000).ToBufferWithCopy()
	for _, tc := range []struct {
		name      string
		build     func([]byte) *Bitmap
		staysOnIt bool
	}{
		{"FromBufferOwned", func(buf []byte) *Bitmap { return FromBufferOwned(nil, buf, nil, nil) }, true},
		{"FromBuffer", FromBuffer, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, len(serialized), len(serialized)*2)
			copy(buf, serialized)

			bm := tc.build(buf)
			require.True(t, bm.Set(999_999_999))
			require.Equal(t, tc.staysOnIt, &buf[0] == &bm.ToBuffer()[0])
			require.True(t, bm.Contains(999_999_999))
		})
	}
}

func TestFromBufferOwned(t *testing.T) {
	src := randomBitmap(1, 10_000, 1_000_000)
	serialized := src.ToBufferWithCopy()

	t.Run("reusing one struct across buffers", func(t *testing.T) {
		a := randomBitmap(2, 100, 10_000)
		b := randomBitmap(3, 200, 10_000)

		var dst Bitmap
		FromBufferOwned(&dst, a.ToBufferWithCopy(), nil, heapPool)
		require.Equal(t, a.ToArray(), dst.ToArray())

		FromBufferOwned(&dst, b.ToBufferWithCopy(), nil, heapPool)
		require.Equal(t, b.ToArray(), dst.ToArray())
	})

	t.Run("mutable within buffer capacity", func(t *testing.T) {
		buf := make([]byte, len(serialized), len(serialized)*2)
		copy(buf, serialized)

		var dst Bitmap
		FromBufferOwned(&dst, buf, nil, heapPool)
		require.True(t, dst.Set(999_999_999))
		require.True(t, dst.Contains(999_999_999))
	})

	t.Run("reinit drops the previous buffer", func(t *testing.T) {
		bufA := randomBitmap(7, 100, 10_000).ToBufferWithCopy()
		bufB := randomBitmap(8, 100, 10_000).ToBufferWithCopy()

		var dst Bitmap
		FromBufferOwned(&dst, bufA, nil, heapPool)
		require.Same(t, &bufA[0], &dst._ptr[0])

		FromBufferOwned(&dst, bufB, nil, heapPool)
		require.Same(t, &bufB[0], &dst._ptr[0])
	})

	t.Run("tiny buffer falls back to empty bitmap", func(t *testing.T) {
		var dst Bitmap
		FromBufferOwned(&dst, make([]byte, 0), nil, heapPool)
		require.True(t, dst.IsEmpty())
		require.True(t, dst.Set(42), "fallback bitmap must be mutable")
	})

	t.Run("tiny buffer fallback allocates only the backing slice", func(t *testing.T) {
		var dst Bitmap
		allocs := testing.AllocsPerRun(100, func() {
			FromBufferOwned(&dst, make([]byte, 0), nil, heapPool)
		})
		require.Equal(t, 1.0, allocs)
	})
}

func TestCloneToPooledStruct(t *testing.T) {
	src := randomBitmap(4, 10_000, 1_000_000)

	t.Run("clone is independent of src", func(t *testing.T) {
		var dst Bitmap
		src.CloneTo(&dst, tailOnly(bufPoolOf(make([]byte, 0, src.LenInBytes()+1024))))
		dst.Set(999_999_998)
		require.True(t, dst.Contains(999_999_998))
		require.False(t, src.Contains(999_999_998))
	})

	t.Run("reusing one struct across clones", func(t *testing.T) {
		a := randomBitmap(5, 100, 10_000)
		b := randomBitmap(6, 200, 10_000)
		buf := make([]byte, 0, max(a.LenInBytes(), b.LenInBytes()))

		var dst Bitmap
		a.CloneTo(&dst, tailOnly(bufPoolOf(buf)))
		require.Equal(t, a.ToArray(), dst.ToArray())

		b.CloneTo(&dst, tailOnly(bufPoolOf(buf)))
		require.Equal(t, b.ToArray(), dst.ToArray())
	})

	t.Run("reinit drops the previous buffer", func(t *testing.T) {
		a := randomBitmap(9, 100, 10_000)
		bufA := make([]byte, 0, a.LenInBytes())
		bufB := make([]byte, 0, a.LenInBytes())

		var dst Bitmap
		a.CloneTo(&dst, tailOnly(bufPoolOf(bufA)))
		require.Same(t, &bufA[:1][0], &dst._ptr[0])

		a.CloneTo(&dst, tailOnly(bufPoolOf(bufB)))
		require.Same(t, &bufB[:1][0], &dst._ptr[0])
	})

	t.Run("nil src initializes empty bitmap over buf", func(t *testing.T) {
		var nilSrc *Bitmap
		var dst Bitmap
		nilSrc.CloneTo(&dst, tailOnly(bufPoolOf(make([]byte, 0, 4096))))
		require.True(t, dst.IsEmpty())
		require.True(t, dst.Set(7))
		require.True(t, dst.Contains(7))
	})

	t.Run("zero-value src initializes empty bitmap over buf", func(t *testing.T) {
		var src, dst Bitmap
		src.CloneTo(&dst, tailOnly(bufPoolOf(make([]byte, 0, 4096))))
		require.True(t, dst.IsEmpty())
		require.True(t, dst.Set(7))
		require.True(t, dst.Contains(7))
	})
}

// trackedPool hands out buffers filled with 0xEE, surplus bytes past the request,
// and overwrites a buffer as soon as it is released, so reading a released buffer
// corrupts the result.
type trackedPool struct {
	surplus int

	mu   sync.Mutex
	bufs []*trackedBuf
}

type trackedBuf struct {
	data     []byte
	released int
}

func (p *trackedPool) Get(minBytes int) ([]byte, func()) {
	b := &trackedBuf{data: fillBytes(make([]byte, minBytes+p.surplus), 0xEE)}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bufs = append(p.bufs, b)
	return b.data, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		b.released++
		fillBytes(b.data, 0xDD)
	}
}

func fillBytes(buf []byte, v byte) []byte {
	for i := range buf {
		buf[i] = v
	}
	return buf
}

// requireReleasedOnce checks live buffers are held and the rest released once.
func (p *trackedPool) requireReleasedOnce(t *testing.T, live int) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	held := 0
	for i, b := range p.bufs {
		require.LessOrEqual(t, b.released, 1, "buffer %d of %d", i, len(p.bufs))
		held += 1 - b.released
	}
	require.Equal(t, live, held)
}

func (p *trackedPool) gets() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.bufs)
}

// smallContainersBitmap has one small array container per key, starting at key first.
func smallContainersBitmap(first, keys int) *Bitmap {
	bm := NewBitmap()
	for k := first; k < first+keys; k++ {
		bm.Set(uint64(k)<<16 | uint64(k))
	}
	return bm
}

// fullContainersBitmap has one bitmap container per key.
func fullContainersBitmap(keys int) *Bitmap {
	return FromSortedList(sortedSeq(keys*maxCardinality/2, 2))
}

func TestPooledGrowthMatchesHeap(t *testing.T) {
	sparse, dense := smallContainersBitmap(1, 300), fullContainersBitmap(3)
	merged := []struct {
		name string
		seed []uint64
		run  func(bm *Bitmap)
	}{
		{name: "Set over new keys", run: func(bm *Bitmap) { bm.SetMany(sparse.ToArray()) }},
		{name: "Set past an array container", seed: []uint64{1 << 16}, run: func(bm *Bitmap) { bm.SetMany(sortedSeq(5000, 3)) }},
		{name: "AndNotOld keeps bitmap containers", seed: dense.ToArray(), run: func(bm *Bitmap) { bm.AndNotOld(FromSortedList([]uint64{2, 1 << 16})) }},
		{name: "FillUp within a key", seed: []uint64{1, 2, 3}, run: func(bm *Bitmap) { bm.FillUp(30_000) }},
		{name: "FillUp across keys", seed: []uint64{1, 2, 3}, run: func(bm *Bitmap) { bm.FillUp(300_000) }},
	}
	for _, tc := range merged {
		t.Run(tc.name, func(t *testing.T) {
			heap := FromSortedList(tc.seed).Clone()
			tc.run(heap)

			pool := &trackedPool{}
			pooled := FromSortedList(tc.seed).CloneTo(nil, tailOnly(pool))
			tc.run(pooled)

			// With no surplus the capacities match, so the layouts do too.
			require.Equal(t, heap.ToBuffer(), pooled.ToBuffer())
			require.Equal(t, heap.CapInBytes(), pooled.CapInBytes())
			pool.requireReleasedOnce(t, 1)
			pooled.ReleaseBuf()
			pool.requireReleasedOnce(t, 0)

			surplusPool := &trackedPool{surplus: 1001}
			surplus := FromSortedList(tc.seed).CloneTo(nil, tailOnly(surplusPool))
			tc.run(surplus)

			require.Equal(t, heap.ToArray(), surplus.ToArray())
			require.Equal(t, heap.Compacted().ToBuffer(), surplus.Compacted().ToBuffer())
			surplus.ReleaseBuf()
			surplusPool.requireReleasedOnce(t, 0)
		})
	}

	t.Run("Masked", func(t *testing.T) {
		masks := uint64(0x0000_0000_00FF_FFFF)
		pool := &trackedPool{}
		got := sparse.maskedInto(masks, NewBitmapTo(nil, tailOnly(pool)), 0)

		require.Greater(t, pool.gets(), 1, "the build must have grown")
		require.Equal(t, sparse.Masked(masks).ToBuffer(), got.ToBuffer())
		pool.requireReleasedOnce(t, 1)
		got.ReleaseBuf()
		pool.requireReleasedOnce(t, 0)
	})
}

// Masked keys that take turns outgrowing their containers leave dead space, so
// the result needs more than the source's size.
func TestMaskedToOutgrowsItsEstimate(t *testing.T) {
	src := NewBitmap()
	for pos := uint64(0); pos < 2; pos++ {
		for key := uint64(0); key < 2; key++ {
			for v := uint64(0); v < 1500; v++ {
				src.Set(pos<<48 | key<<16 | (2*v + pos))
			}
		}
	}
	src = src.Compacted()
	masked := uint64(0x0000FFFFFFFFFFFF)
	for _, tailOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("tail only %t", tailOnly), func(t *testing.T) {
			pool := &trackedPool{}
			got := src.MaskedTo(nil, masked, BuildOpts{Pool: pool, InitialSpareRatio: 0.1, SpareTailOnly: tailOnly})

			require.Greater(t, pool.gets(), 1, "the build must have grown")
			require.Equal(t, src.Masked(masked).ToArray(), got.ToArray())
			pool.requireReleasedOnce(t, 1)
			got.ReleaseBuf()
			pool.requireReleasedOnce(t, 0)
		})
	}
}

// A build's spare capacity is the pool's old bytes, so each way of growing into
// it has to overwrite what it exposes.
func TestGrowthIntoDirtyMemory(t *testing.T) {
	array, dense := fillKeys(30, 100), fullContainersBitmap(40)
	overlap := bitmapOf(append(sortedSeq(1500, 3), 5<<16, 1<<40)...)
	ops := []struct {
		name string
		run  func(bm *Bitmap)
	}{
		{"Set at new keys", func(bm *Bitmap) { bm.SetMany(array.ToArray()) }},
		{"Set descending", func(bm *Bitmap) {
			for k := uint64(50); k > 0; k-- {
				bm.Set(k << 16)
			}
		}},
		{"Set past an array", func(bm *Bitmap) { bm.SetMany(sortedSeq(5000, 3)) }},
		{"Set past arrays, interleaved", func(bm *Bitmap) {
			for v := uint64(0); v < 3000; v++ {
				bm.Set(v)
				bm.Set(1<<16 | v)
			}
		}},
		{"Set, Remove, Set", func(bm *Bitmap) {
			bm.SetMany(sortedSeq(1000, 1))
			bm.RemoveRange(10, 990)
			bm.SetMany(sortedSeq(1200, 2))
		}},
		{"Or arrays", func(bm *Bitmap) { bm.Or(array) }},
		{"Or bitmaps", func(bm *Bitmap) { bm.Or(dense) }},
		{"Or grows containers", func(bm *Bitmap) { bm.Or(overlap).Or(bitmapOf(sortedSeq(2500, 7)...)) }},
		{"Or over many keys", func(bm *Bitmap) { bm.Or(array).Or(dense) }},
		{"OrConc", func(bm *Bitmap) { bm.OrConc(array, 4).OrConc(dense, 4) }},
		{"OrOld", func(bm *Bitmap) { bm.OrOld(array); bm.OrOld(dense) }},
		{"Or, AndNot", func(bm *Bitmap) { bm.Or(dense).AndNot(array) }},
		{"Or, AndNotConc", func(bm *Bitmap) { bm.Or(dense).AndNotConc(array, 4) }},
		{"Or, AndNotOld", func(bm *Bitmap) { bm.Or(dense).AndNotOld(array) }},
		{"Or, And", func(bm *Bitmap) { bm.Or(dense).And(array) }},
		{"Or, AndOld", func(bm *Bitmap) { bm.Or(dense).AndOld(array) }},
		{"Or, Cleanup, Or", func(bm *Bitmap) {
			bm.Or(array).AndNot(array)
			bm.Cleanup()
			bm.Or(overlap)
		}},
		{"ConvertToBitmapContainers", func(bm *Bitmap) { bm.Or(overlap).ConvertToBitmapContainers() }},
		{"FillUp within a key", func(bm *Bitmap) { bm.FillUp(30_000) }},
		{"FillUp to a key's end", func(bm *Bitmap) { bm.FillUp(65_535) }},
		{"FillUp across keys", func(bm *Bitmap) { bm.FillUp(300_000) }},
		{"FillUp past the old keys node", func(bm *Bitmap) { bm.FillUp(700_000) }},
		{"FillUp after emptying", func(bm *Bitmap) { bm.Or(dense); bm.ZeroOut(); bm.FillUp(700_000) }},
		{"FillUp after values", func(bm *Bitmap) { bm.Set(70_000); bm.FillUp(300_000) }},
		{"Reset, Set", func(bm *Bitmap) { bm.Or(array).Reset(); bm.SetMany(sortedSeq(500, 1<<14)) }},
	}
	seed := bitmapOf(1, 2, 3, 2<<16)
	builds := []struct {
		name  string
		build func(opts BuildOpts) *Bitmap
	}{
		{"NewBitmapTo", func(opts BuildOpts) *Bitmap { return NewBitmapTo(nil, opts) }},
		{"CompactedTo", func(opts BuildOpts) *Bitmap { return seed.CompactedTo(nil, opts) }},
		{"CloneTo", func(opts BuildOpts) *Bitmap { return seed.CloneTo(nil, opts) }},
	}
	for _, b := range builds {
		for _, op := range ops {
			// No surplus grows through the pool, 512 KiB stays in the first buffer.
			for _, surplus := range []int{0, 5000, 1 << 19} {
				for _, tailOnly := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/surplus %d/tail only %t", b.name, op.name, surplus, tailOnly), func(t *testing.T) {
						clean := b.build(BuildOpts{SpareTailOnly: tailOnly, Pool: getPool(func(n int) []byte { return make([]byte, n+surplus) })})
						dirty := b.build(BuildOpts{SpareTailOnly: tailOnly, Pool: &trackedPool{surplus: surplus}})

						op.run(clean)
						op.run(dirty)

						require.Equal(t, clean.ToArray(), dirty.ToArray())
						require.True(t, slices.Equal(clean.data, dirty.data), "growth shows the buffers' old bytes")
					})
				}
			}
		}
	}
}

func TestReleaseBuf(t *testing.T) {
	t.Run("twice releases once", func(t *testing.T) {
		pool := &trackedPool{}
		bm := NewBitmapTo(nil, tailOnly(pool))
		bm.ReleaseBuf()
		bm.ReleaseBuf()
		pool.requireReleasedOnce(t, 0)
	})

	t.Run("nil is a no-op", func(t *testing.T) {
		var bm *Bitmap
		bm.ReleaseBuf()
	})
}

func byteSliceOf(data []uint16) []byte { return uint16ToByteSliceUnsafe(data) }

// toBuild is a build taking BuildOpts.
type toBuild struct {
	// name is the exported method a panic reports, label what tells apart the
	// cases of one method.
	name, label string
	build       func(dst *Bitmap, opts BuildOpts) *Bitmap
	heap        func() *Bitmap
	// size is the bytes asked for at InitialSpareRatio 0.
	size int
	// layout is the bitmap whose containers price a key slot, nil for an empty one.
	layout *Bitmap
	// src is a source dst must not be, nil if there is none.
	src *Bitmap
}

// toBuilds covers each build taking BuildOpts.
func toBuilds() []toBuild {
	a, b := bitmapOf(1, 2, 3, 1<<20), bitmapOf(2, 3, 1<<21)
	wide, empty := fillKeys(20, 100), NewBitmap()
	masked := uint64(0x0000_0000_0007_FFFF)
	acc, wideAcc := NewAccumulator(), NewAccumulator()
	acc.Or(a)
	wideAcc.Or(wide)
	vals32 := []uint32{1, 2, 3, 1 << 20}
	tiny := []byte{1, 2, 3, 4, 5, 6}
	emptySize := empty.LenInBytes()
	list, wideList := a.ToArray(), wide.ToArray()
	wideBytes := wide.ToBufferWithCopy()
	pair, wides := []*Bitmap{a, a}, []*Bitmap{a, wide, a}

	builds := []toBuild{
		{name: "NewBitmapTo", build: NewBitmapTo, heap: NewBitmap, size: emptySize},
		{name: "FromSortedListTo", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return FromSortedListTo(dst, list, opts)
		}, heap: func() *Bitmap { return FromSortedList(list) }, size: FromSortedList(list).LenInBytes(), layout: FromSortedList(list)},
		{name: "FromSortedListTo", label: "many keys", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return FromSortedListTo(dst, wideList, opts)
		}, heap: func() *Bitmap { return FromSortedList(wideList) }, size: FromSortedList(wideList).LenInBytes(), layout: FromSortedList(wideList)},
		{name: "FromSortedList32To", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return FromSortedList32To(dst, vals32, opts)
		}, heap: func() *Bitmap { return FromSortedList32(vals32) }, size: FromSortedList32(vals32).LenInBytes(), layout: FromSortedList32(vals32)},
		{name: "Accumulator.BitmapTo", build: acc.BitmapTo, heap: acc.Bitmap, size: acc.Bitmap().LenInBytes(), layout: acc.Bitmap()},
		{name: "Accumulator.BitmapTo", label: "many keys", build: wideAcc.BitmapTo, heap: wideAcc.Bitmap, size: wideAcc.Bitmap().LenInBytes(), layout: wideAcc.Bitmap()},

		{name: "FromBufferWithCopyTo", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return FromBufferWithCopyTo(dst, wideBytes, opts)
		}, heap: func() *Bitmap { return FromBufferWithCopy(wideBytes) }, size: len(wideBytes), layout: wide},
		{name: "FromBufferWithCopyTo", label: "under 8 bytes", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return FromBufferWithCopyTo(dst, tiny, opts)
		}, heap: func() *Bitmap { return FromBufferWithCopy(tiny) }, size: emptySize},
		{name: "FromBufferWithCopyTo", label: "nil", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return FromBufferWithCopyTo(dst, nil, opts)
		}, heap: func() *Bitmap { return FromBufferWithCopy(nil) }, size: emptySize},

		{name: "MaskedTo", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return a.MaskedTo(dst, math.MaxUint64, opts)
		}, heap: func() *Bitmap { return a.Masked(math.MaxUint64) }, size: emptySize + a.LenInBytes(), layout: a, src: a},
		{name: "MaskedTo", label: "keys collapse", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return wide.MaskedTo(dst, masked, opts)
		}, heap: func() *Bitmap { return wide.Masked(masked) }, size: emptySize + wide.LenInBytes(), layout: wide, src: wide},
		{name: "MaskedTo", label: "nil", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return (*Bitmap)(nil).MaskedTo(dst, masked, opts)
		}, heap: NewBitmap, size: emptySize},
		{name: "MaskedAndTo", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return MaskedAndTo(dst, a, b, math.MaxUint64, opts)
		}, heap: func() *Bitmap { return MaskedAnd(a, b, math.MaxUint64) }, size: emptySize + b.LenInBytes(), layout: b, src: a},
		{name: "MaskedAndTo", label: "smaller second", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return MaskedAndTo(dst, wide, b, masked, opts)
		}, heap: func() *Bitmap { return MaskedAnd(wide, b, masked) }, size: emptySize + b.LenInBytes(), layout: b, src: b},
		{name: "MaskedAndTo", label: "empty", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return MaskedAndTo(dst, empty, b, masked, opts)
		}, heap: NewBitmap, size: emptySize + empty.LenInBytes(), layout: empty, src: empty},
		{name: "MaskedAndTo", label: "nil", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return MaskedAndTo(dst, nil, nil, masked, opts)
		}, heap: NewBitmap, size: emptySize},
		{name: "CopresenceByMaskTo", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return CopresenceByMaskTo(dst, pair, math.MaxUint64, opts)
		}, heap: func() *Bitmap { return CopresenceByMask(pair, math.MaxUint64) }, size: emptySize + a.LenInBytes(), layout: a, src: a},
		{name: "CopresenceByMaskTo", label: "largest in the middle", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return CopresenceByMaskTo(dst, wides, math.MaxUint64, opts)
		}, heap: func() *Bitmap { return CopresenceByMask(wides, math.MaxUint64) }, size: emptySize + wide.LenInBytes(), layout: wide, src: a},
		{name: "CopresenceByMaskTo", label: "single", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return CopresenceByMaskTo(dst, []*Bitmap{a}, math.MaxUint64, opts)
		}, heap: func() *Bitmap { return a.Clone() }, size: a.LenInBytes(), layout: a, src: a},
		{name: "CopresenceByMaskTo", label: "empty input", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return CopresenceByMaskTo(dst, []*Bitmap{a, empty}, math.MaxUint64, opts)
		}, heap: NewBitmap, size: emptySize, src: a},
		{name: "CopresenceByMaskTo", label: "none", build: func(dst *Bitmap, opts BuildOpts) *Bitmap {
			return CopresenceByMaskTo(dst, nil, math.MaxUint64, opts)
		}, heap: NewBitmap, size: emptySize},
	}

	for _, src := range []struct {
		label string
		bm    *Bitmap
	}{{"", a}, {"many keys", bitmapOf(sortedSeq(20, 1<<16)...)}, {"one value per key and more", fillKeys(3, 100)}, {"empty", NewBitmap()}, {"zero value", &Bitmap{}}, {"nil", nil}} {
		bm := src.bm
		layout, cloneSize := bm, bm.LenInBytes()
		if cloneSize == 0 {
			layout, cloneSize = nil, emptySize
		}
		builds = append(builds,
			toBuild{name: "CloneTo", label: src.label, build: bm.CloneTo, heap: func() *Bitmap {
				if bm.LenInBytes() == 0 {
					return NewBitmap()
				}
				return bm.Clone()
			}, size: cloneSize, layout: layout, src: bm},
			toBuild{name: "CompactedTo", label: src.label, build: bm.CompactedTo, heap: bm.Compacted, size: bm.Compacted().LenInBytes(), layout: bm.Compacted(), src: bm},
		)
	}
	return builds
}

// keyRoom is how many keys bm's keys node holds, read from its serialized
// bytes, which a later FromBuffer sees.
func keyRoom(bm *Bitmap) int {
	size := binary.LittleEndian.Uint64(uint16ToByteSliceUnsafe(bm.data))
	return (int(size)/4 - indexNodeStart) / 2
}

// growUntilGet adds keys to bm until pool has served gets buffers, and returns
// what it added.
func growUntilGet(t *testing.T, bm *Bitmap, pool *trackedPool, gets int) (added []uint64) {
	t.Helper()
	for k := uint64(1000); pool.gets() < gets; k++ {
		require.Less(t, k, uint64(5000), "the result never grew")
		bm.Set(k << 16)
		added = append(added, k<<16)
	}
	return added
}

func TestBuildsTo(t *testing.T) {
	const surplus = 1501
	for _, tc := range toBuilds() {
		want := tc.heap().ToArray()
		tailKeyRoom := keyRoom(tc.build(nil, BuildOpts{SpareTailOnly: true}))
		for _, pooled := range []bool{false, true} {
			for _, ratio := range []float64{0, 1.3001} {
				for _, tailOnly := range []bool{false, true} {
					for _, reuse := range []bool{false, true} {
						name := fmt.Sprintf("%s/%s/pooled %t/ratio %v/tail only %t/reuse %t", tc.name, tc.label, pooled, ratio, tailOnly, reuse)
						t.Run(name, func(t *testing.T) {
							size := int(math.Ceil(float64(tc.size) * (1 + ratio)))
							if ratio > 0 {
								require.NotEqual(t, float64(size), float64(tc.size)*(1+ratio), "must be fractional to catch truncation")
							}
							opts := BuildOpts{InitialSpareRatio: ratio, SpareTailOnly: tailOnly}
							pool, capBytes := &trackedPool{surplus: surplus}, size/2*2
							if pooled {
								opts.Pool = pool
								capBytes = (size + surplus) / 2 * 2
							}
							var dst *Bitmap
							if reuse {
								dst = FromBufferOwned(nil, bitmapOf(99, 100_000).ToBuffer(), nil, heapPool)
							}

							got := tc.build(dst, opts)

							if reuse {
								require.Same(t, dst, got)
							}
							require.Equal(t, want, got.ToArray())
							require.Equal(t, capBytes, got.CapInBytes())
							keys := tailKeyRoom
							if !tailOnly {
								keys += spareExtraKeys(tc.layout, (capBytes-tc.size)/2)
							}
							require.Equal(t, keys, keyRoom(got))
							require.Equal(t, keys, got.keys.maxKeys())

							if !pooled {
								require.Nil(t, got.pool)
								require.Nil(t, got.releaseBuf)
								grown := bitmapOf(append(sortedSeq(1000, 1<<16), want...)...)
								got.SetMany(grown.ToArray())
								require.Nil(t, got.pool)
								require.Nil(t, got.releaseBuf)
								require.Equal(t, grown.ToArray(), got.ToArray())
								return
							}

							require.Equal(t, 1, pool.gets())
							require.Len(t, pool.bufs[0].data, size+surplus)
							require.Same(t, &pool.bufs[0].data[0], &got._ptr[0])
							cleanOpts := opts
							cleanOpts.Pool = getPool(func(n int) []byte { return make([]byte, n+surplus) })
							clean := tc.build(nil, cleanOpts)
							require.Equal(t, clean.data, got.data, "the buffer's old bytes show")

							// A buffer as large bought by surplus alone grows the same way.
							unpadded := &trackedPool{surplus: surplus + size - tc.size}
							ref := tc.build(nil, BuildOpts{Pool: unpadded, SpareTailOnly: tailOnly})
							unpadded.surplus = surplus
							for _, x := range growUntilGet(t, got, pool, 2) {
								clean.Set(x)
							}
							require.Equal(t, clean.data, got.data, "growth shows the buffers' old bytes")
							growUntilGet(t, ref, unpadded, 2)
							require.Len(t, pool.bufs[1].data, len(unpadded.bufs[1].data))
							require.Equal(t, ref.ToArray(), got.ToArray())
							pool.requireReleasedOnce(t, 1)
							got.ReleaseBuf()
							pool.requireReleasedOnce(t, 0)
						})
					}
				}
			}
		}
	}
}

func TestBuildsToPanics(t *testing.T) {
	for _, tc := range toBuilds() {
		for _, ratio := range []float64{-0.01, math.NaN()} {
			want := fmt.Sprintf("%s: InitialSpareRatio must not be negative or NaN, got %v", tc.name, ratio)
			for _, pool := range []BufPool{nil, heapPool} {
				t.Run(fmt.Sprintf("%s/%s/pooled %t", want, tc.label, pool != nil), func(t *testing.T) {
					require.PanicsWithValue(t, want, func() { tc.build(nil, BuildOpts{Pool: pool, InitialSpareRatio: ratio}) })
				})
			}
		}

		// A pool returning the unpadded size fails the padded request.
		padded := int(math.Ceil(float64(tc.size) * 1.5))
		for _, small := range []struct {
			ratio     float64
			need, got int
		}{{0, tc.size, 4}, {0, tc.size, tc.size - 2}, {0.5, padded, tc.size}} {
			want := fmt.Sprintf("%s: buf too small: need %d bytes, got %d", tc.name, small.need, small.got)
			t.Run(want+"/"+tc.label, func(t *testing.T) {
				pool := getPool(func(int) []byte { return make([]byte, small.got) })
				require.PanicsWithValue(t, want, func() { tc.build(nil, BuildOpts{Pool: pool, InitialSpareRatio: small.ratio}) })
			})
		}

		if tc.src != nil {
			t.Run(fmt.Sprintf("%s/%s/dst is a source", tc.name, tc.label), func(t *testing.T) {
				before := *tc.src
				for _, pool := range []BufPool{nil, heapPool} {
					requirePanicPrefix(t, tc.name+": dst is ", func() { tc.build(tc.src, BuildOpts{Pool: pool}) })
				}
				require.Equal(t, before, *tc.src)
			})
		}
	}
}

func TestBuildsToAllocateNothing(t *testing.T) {
	buf, dst := make([]byte, 1<<16), &Bitmap{}
	pool := getPool(func(int) []byte { return buf })
	for _, tc := range toBuilds() {
		// Copresence allocates the entries it walks, as its heap form does.
		if tc.name == "CopresenceByMaskTo" {
			continue
		}
		for _, opts := range []BuildOpts{{Pool: pool}, {Pool: pool, SpareTailOnly: true}, {Pool: pool, InitialSpareRatio: 0.3}} {
			require.Zero(t, testing.AllocsPerRun(10, func() { tc.build(dst, opts) }), "%s %s", tc.name, tc.label)
		}
	}
}

func TestCloneToZeroOptsSerializesAsClone(t *testing.T) {
	for _, tc := range layoutSources(t) {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.src.Clone().ToBuffer(), tc.src.CloneTo(nil, BuildOpts{}).ToBuffer())
		})
	}
}

// expandKeys caps its growth at an odd number of uint16s, which the keys node's
// uint64 view leaves a remainder of.
func TestSplitClearsOddKeysNodeRemainder(t *testing.T) {
	src := fillKeys(3, 100)
	src.expandKeys(math.MaxUint16)
	require.NotZero(t, src.keys.size()%4)

	got := src.CloneTo(nil, BuildOpts{Pool: &trackedPool{surplus: 1 << 12}})

	require.Greater(t, got.keys.size(), src.keys.size())
	require.Equal(t, make([]uint16, got.keys.size()%4), got.data[got.keys.size()/4*4:got.keys.size()])
	require.Equal(t, src.ToArray(), FromBuffer(got.ToBuffer()).ToArray())
}

func TestFromBufferWithCopyToDoesNotAlias(t *testing.T) {
	want := fillKeys(20, 100).ToArray()
	for _, opts := range []BuildOpts{{}, {InitialSpareRatio: 1}, {InitialSpareRatio: 1, SpareTailOnly: true}, {Pool: &trackedPool{}}, {Pool: &trackedPool{surplus: 1 << 12}}} {
		t.Run(fmt.Sprintf("%+v", opts), func(t *testing.T) {
			// The spare capacity must not become the result's tail room.
			src := append(make([]byte, 0, 1<<16), fillKeys(20, 100).ToBuffer()...)
			before := append([]byte{}, src[:cap(src)]...)

			got := FromBufferWithCopyTo(nil, src, opts)
			got.Set(50)
			got.Set(1 << 40)
			got.Remove(50)
			got.Remove(1 << 40)

			require.Equal(t, before, src[:cap(src)])
			fillBytes(src, 0xDD)
			require.Equal(t, want, got.ToArray())
		})
	}
}

// An odd length is fatal rather than a panic, so it takes a process of its own.
func TestFromBufferWithCopyToOddLength(t *testing.T) {
	const env = "SROAR_ODD_LENGTH"
	if v := os.Getenv(env); v != "" {
		n, _ := strconv.Atoi(v)
		FromBufferWithCopyTo(nil, make([]byte, n), BuildOpts{})
		return
	}
	for _, n := range []string{"7", "17"} {
		t.Run(n, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestFromBufferWithCopyToOddLength$", "-test.timeout=20s")
			cmd.Env = append(os.Environ(), env+"="+n)
			out, err := cmd.CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(out), "Assertion failure")
		})
	}
}

func TestFromBufferOwnedShortBufferTakesPoolBuffer(t *testing.T) {
	pool := &trackedPool{}
	short, release := pool.Get(4)
	var dst Bitmap

	got := FromBufferOwned(&dst, short, release, pool)

	require.Same(t, &dst, got)
	require.True(t, got.IsEmpty())
	require.Equal(t, 2, pool.gets())
	require.Equal(t, 1, pool.bufs[0].released)
	require.Same(t, &pool.bufs[1].data[0], &got._ptr[0])

	got.SetMany(sortedSeq(10_000, 1<<14))
	require.Equal(t, sortedSeq(10_000, 1<<14), got.ToArray())
	pool.requireReleasedOnce(t, 1)
	got.ReleaseBuf()
	pool.requireReleasedOnce(t, 0)
}

func TestFromBufferOwnedOwnership(t *testing.T) {
	src := smallContainersBitmap(1, 20)
	const header = 16

	// A node buffer holds a header, the serialized bitmap, then spare capacity.
	newNode := func(pool *trackedPool, spare int) (*trackedBuf, []byte, func()) {
		buf, release := pool.Get(header + src.LenInBytes() + spare)
		copy(buf[header:], src.ToBuffer())
		return pool.bufs[0], buf[header : header+src.LenInBytes()], release
	}

	t.Run("sub-slice grows within the whole buffer", func(t *testing.T) {
		pool := &trackedPool{}
		node, additions, release := newNode(pool, 1<<16)
		var dst Bitmap
		bm := FromBufferOwned(&dst, additions, release, pool)

		require.Same(t, &dst, bm)
		require.Same(t, &node.data[header], &byteSliceOf(bm.data)[0])
		require.Equal(t, cap(additions), bm.CapInBytes())
		bm.Set(1 << 40)

		require.Equal(t, 1, pool.gets())
		require.Equal(t, fillBytes(make([]byte, header), 0xEE), node.data[:header], "the header must stay put")
		bm.ReleaseBuf()
		pool.requireReleasedOnce(t, 0)
	})

	t.Run("sub-slice outgrows the buffer", func(t *testing.T) {
		pool := &trackedPool{}
		_, additions, release := newNode(pool, 0)
		bm := FromBufferOwned(nil, additions, release, pool)
		want := NewBitmap()
		want.SetMany(src.ToArray())
		more := sortedSeq(10_000, 1<<14)

		bm.SetMany(more)
		want.SetMany(more)

		require.Greater(t, pool.gets(), 1)
		require.Equal(t, want.ToArray(), bm.ToArray())
		pool.requireReleasedOnce(t, 1)
		bm.ReleaseBuf()
		pool.requireReleasedOnce(t, 0)
	})

	t.Run("nil pool grows on the heap and releases", func(t *testing.T) {
		pool := &trackedPool{}
		_, additions, release := newNode(pool, 0)
		bm := FromBufferOwned(nil, additions, release, nil)
		pool.requireReleasedOnce(t, 1)

		bm.Set(1 << 40)

		require.Equal(t, 1, pool.gets())
		pool.requireReleasedOnce(t, 0)
		require.Nil(t, bm.pool)
		require.True(t, bm.Contains(1<<40))
		require.Equal(t, src.GetCardinality()+1, bm.GetCardinality())
	})

	t.Run("nil pool releases through ReleaseBuf", func(t *testing.T) {
		pool := &trackedPool{}
		_, additions, release := newNode(pool, 0)
		bm := FromBufferOwned(nil, additions, release, nil)
		bm.ReleaseBuf()
		bm.ReleaseBuf()
		pool.requireReleasedOnce(t, 0)
	})

	t.Run("nil pool and short buffer", func(t *testing.T) {
		pool := &trackedPool{}
		short, release := pool.Get(4)
		var dst Bitmap
		bm := FromBufferOwned(&dst, short, release, nil)

		require.Same(t, &dst, bm)
		require.Equal(t, 1, pool.bufs[0].released)
		require.Equal(t, 1, pool.gets())
		require.Nil(t, bm.pool)
		require.Nil(t, bm.releaseBuf)
		require.Equal(t, emptyBitmapBytes, bm.CapInBytes())
		bm.SetMany(sortedSeq(10_000, 1<<14))
		require.Equal(t, sortedSeq(10_000, 1<<14), bm.ToArray())
	})

	t.Run("short buffer keeps the pool's surplus as tail room", func(t *testing.T) {
		bm := FromBufferOwned(nil, nil, nil, &trackedPool{surplus: 1 << 12})
		require.Equal(t, NewBitmap().keys.maxKeys(), bm.keys.maxKeys())
	})
}
