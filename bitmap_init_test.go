package sroar

import (
	"fmt"
	"math"
	"math/rand"
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

func TestFromBufferWithCapGrowsIntoSpareCapacity(t *testing.T) {
	serialized := randomBitmap(1, 10_000, 1_000_000).ToBufferWithCopy()
	for _, tc := range []struct {
		name      string
		build     func([]byte) *Bitmap
		staysOnIt bool
	}{
		{"FromBufferWithCap", FromBufferWithCap, true},
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

func TestFromBufferToBuf(t *testing.T) {
	src := randomBitmap(1, 10_000, 1_000_000)
	serialized := src.ToBufferWithCopy()

	t.Run("reusing one struct across buffers", func(t *testing.T) {
		a := randomBitmap(2, 100, 10_000)
		b := randomBitmap(3, 200, 10_000)

		var dst Bitmap
		FromBufferToBuf(&dst, a.ToBufferWithCopy(), nil, heapPool)
		require.Equal(t, a.ToArray(), dst.ToArray())

		FromBufferToBuf(&dst, b.ToBufferWithCopy(), nil, heapPool)
		require.Equal(t, b.ToArray(), dst.ToArray())
	})

	t.Run("mutable within buffer capacity", func(t *testing.T) {
		buf := make([]byte, len(serialized), len(serialized)*2)
		copy(buf, serialized)

		var dst Bitmap
		FromBufferToBuf(&dst, buf, nil, heapPool)
		require.True(t, dst.Set(999_999_999))
		require.True(t, dst.Contains(999_999_999))
	})

	t.Run("reinit drops the previous buffer", func(t *testing.T) {
		bufA := randomBitmap(7, 100, 10_000).ToBufferWithCopy()
		bufB := randomBitmap(8, 100, 10_000).ToBufferWithCopy()

		var dst Bitmap
		FromBufferToBuf(&dst, bufA, nil, heapPool)
		require.Same(t, &bufA[0], &dst._ptr[0])

		FromBufferToBuf(&dst, bufB, nil, heapPool)
		require.Same(t, &bufB[0], &dst._ptr[0])
	})

	t.Run("tiny buffer falls back to empty bitmap", func(t *testing.T) {
		var dst Bitmap
		FromBufferToBuf(&dst, make([]byte, 0), nil, heapPool)
		require.True(t, dst.IsEmpty())
		require.True(t, dst.Set(42), "fallback bitmap must be mutable")
	})

	t.Run("tiny buffer fallback allocates only the backing slice", func(t *testing.T) {
		var dst Bitmap
		allocs := testing.AllocsPerRun(100, func() {
			FromBufferToBuf(&dst, make([]byte, 0), nil, heapPool)
		})
		require.Equal(t, 1.0, allocs)
	})
}

func TestCloneToBufPooledStruct(t *testing.T) {
	src := randomBitmap(4, 10_000, 1_000_000)

	t.Run("clone is independent of src", func(t *testing.T) {
		var dst Bitmap
		src.CloneToBuf(&dst, bufPoolOf(make([]byte, 0, src.LenInBytes()+1024)))
		dst.Set(999_999_998)
		require.True(t, dst.Contains(999_999_998))
		require.False(t, src.Contains(999_999_998))
	})

	t.Run("reusing one struct across clones", func(t *testing.T) {
		a := randomBitmap(5, 100, 10_000)
		b := randomBitmap(6, 200, 10_000)
		buf := make([]byte, 0, max(a.LenInBytes(), b.LenInBytes()))

		var dst Bitmap
		a.CloneToBuf(&dst, bufPoolOf(buf))
		require.Equal(t, a.ToArray(), dst.ToArray())

		b.CloneToBuf(&dst, bufPoolOf(buf))
		require.Equal(t, b.ToArray(), dst.ToArray())
	})

	t.Run("reinit drops the previous buffer", func(t *testing.T) {
		a := randomBitmap(9, 100, 10_000)
		bufA := make([]byte, 0, a.LenInBytes())
		bufB := make([]byte, 0, a.LenInBytes())

		var dst Bitmap
		a.CloneToBuf(&dst, bufPoolOf(bufA))
		require.Same(t, &bufA[:1][0], &dst._ptr[0])

		a.CloneToBuf(&dst, bufPoolOf(bufB))
		require.Same(t, &bufB[:1][0], &dst._ptr[0])
	})

	t.Run("nil src initializes empty bitmap over buf", func(t *testing.T) {
		var nilSrc *Bitmap
		var dst Bitmap
		nilSrc.CloneToBuf(&dst, bufPoolOf(make([]byte, 0, 4096)))
		require.True(t, dst.IsEmpty())
		require.True(t, dst.Set(7))
		require.True(t, dst.Contains(7))
	})

	t.Run("zero-value src initializes empty bitmap over buf", func(t *testing.T) {
		var src, dst Bitmap
		src.CloneToBuf(&dst, bufPoolOf(make([]byte, 0, 4096)))
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
			pooled := FromSortedList(tc.seed).CloneToBuf(nil, pool)
			tc.run(pooled)

			// With no surplus the capacities match, so the layouts do too.
			require.Equal(t, heap.ToBuffer(), pooled.ToBuffer())
			require.Equal(t, heap.CapInBytes(), pooled.CapInBytes())
			pool.requireReleasedOnce(t, 1)
			pooled.ReleaseBuf()
			pool.requireReleasedOnce(t, 0)

			surplusPool := &trackedPool{surplus: 1001}
			surplus := FromSortedList(tc.seed).CloneToBuf(nil, surplusPool)
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
		got := sparse.maskedInto(masks, NewBitmapToBuf(nil, pool))

		require.Greater(t, pool.gets(), 1, "the build must have grown")
		require.Equal(t, sparse.Masked(masks).ToBuffer(), got.ToBuffer())
		pool.requireReleasedOnce(t, 1)
		got.ReleaseBuf()
		pool.requireReleasedOnce(t, 0)
	})
}

func TestReleaseBuf(t *testing.T) {
	t.Run("twice releases once", func(t *testing.T) {
		pool := &trackedPool{}
		bm := NewBitmapToBuf(nil, pool)
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

// toBufCases are the ToBuf constructors that take a buffer from the pool once,
// with what each builds.
func toBufCases() []struct {
	name string
	call func(dst *Bitmap, pool BufPool) *Bitmap
	want []uint64
} {
	a := bitmapOf(1, 2, 3, 1<<20)
	b := bitmapOf(2, 3, 1<<21)
	acc := NewAccumulator()
	acc.Or(a)
	return []struct {
		name string
		call func(dst *Bitmap, pool BufPool) *Bitmap
		want []uint64
	}{
		{"NewBitmapToBuf", func(dst *Bitmap, pool BufPool) *Bitmap {
			return NewBitmapToBuf(dst, pool)
		}, []uint64{}},
		{"CloneToBuf", func(dst *Bitmap, pool BufPool) *Bitmap { return a.CloneToBuf(dst, pool) }, a.ToArray()},
		{"CloneToBuf", func(dst *Bitmap, pool BufPool) *Bitmap { return (*Bitmap)(nil).CloneToBuf(dst, pool) }, []uint64{}},
		{"CompactedToBuf", func(dst *Bitmap, pool BufPool) *Bitmap { return a.CompactedToBuf(dst, pool) }, a.ToArray()},
		{"CompactedToBufWithHeadroom", func(dst *Bitmap, pool BufPool) *Bitmap {
			return a.CompactedToBufWithHeadroom(dst, pool)
		}, a.ToArray()},
		{"Accumulator.BitmapToBuf", func(dst *Bitmap, pool BufPool) *Bitmap { return acc.BitmapToBuf(dst, pool) }, a.ToArray()},
		{"FromSortedListToBuf", func(dst *Bitmap, pool BufPool) *Bitmap {
			return FromSortedListToBuf(dst, a.ToArray(), pool)
		}, a.ToArray()},
		{"FromSortedList32ToBuf", func(dst *Bitmap, pool BufPool) *Bitmap {
			return FromSortedList32ToBuf(dst, []uint32{1, 2, 3, 1 << 20}, pool)
		}, a.ToArray()},
		{"MaskedToBuf", func(dst *Bitmap, pool BufPool) *Bitmap {
			return a.MaskedToBuf(dst, math.MaxUint64, pool)
		}, a.ToArray()},
		{"MaskedAndToBuf", func(dst *Bitmap, pool BufPool) *Bitmap {
			return MaskedAndToBuf(dst, a, b, math.MaxUint64, pool)
		}, []uint64{2, 3}},
		{"CopresenceByMaskToBuf", func(dst *Bitmap, pool BufPool) *Bitmap {
			return CopresenceByMaskToBuf(dst, []*Bitmap{a, a}, math.MaxUint64, pool)
		}, a.ToArray()},
		{"CopresenceByMaskToBuf", func(dst *Bitmap, pool BufPool) *Bitmap {
			return CopresenceByMaskToBuf(dst, []*Bitmap{a}, math.MaxUint64, pool)
		}, a.ToArray()},
		{"CopresenceByMaskToBuf", func(dst *Bitmap, pool BufPool) *Bitmap {
			return CopresenceByMaskToBuf(dst, nil, math.MaxUint64, pool)
		}, []uint64{}},
	}
}

func TestToBufConstructors(t *testing.T) {
	for i, tc := range toBufCases() {
		t.Run(fmt.Sprintf("%s/%d", tc.name, i), func(t *testing.T) {
			pool := &trackedPool{}
			var dst Bitmap
			got := tc.call(&dst, pool)

			require.Same(t, &dst, got)
			require.Equal(t, tc.want, got.ToArray())
			require.Equal(t, 1, pool.gets())
			require.Same(t, &pool.bufs[0].data[0], &got._ptr[0])

			// Growth runs through the pool the constructor was given.
			got.SetMany(sortedSeq(10_000, 1<<14))
			require.Greater(t, pool.gets(), 1)
			pool.requireReleasedOnce(t, 1)
			got.ReleaseBuf()
			pool.requireReleasedOnce(t, 0)

			fresh := tc.call(nil, &trackedPool{})
			require.NotNil(t, fresh)
			require.Equal(t, tc.want, fresh.ToArray())

			require.PanicsWithValue(t, tc.name+": pool is nil", func() { tc.call(nil, nil) })
			requirePanicPrefix(t, tc.name+": buf too small: need ", func() {
				tc.call(nil, getPool(func(int) []byte { return make([]byte, 4) }))
			})
		})
	}
}

func TestFromBufferToBufShortBufferTakesPoolBuffer(t *testing.T) {
	pool := &trackedPool{}
	short, release := pool.Get(4)
	var dst Bitmap

	got := FromBufferToBuf(&dst, short, release, pool)

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

func TestFromBufferToBufOwnership(t *testing.T) {
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
		bm := FromBufferToBuf(&dst, additions, release, pool)

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
		bm := FromBufferToBuf(nil, additions, release, pool)
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

	t.Run("nil pool panics", func(t *testing.T) {
		require.PanicsWithValue(t, "FromBufferToBuf: pool is nil", func() {
			FromBufferToBuf(nil, src.Clone().ToBuffer(), nil, nil)
		})
	})
}
