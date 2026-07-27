package sroar

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func randomBitmap(seed int64, n int, universe uint64) *Bitmap {
	rng := rand.New(rand.NewSource(seed))
	bm := NewBitmap()
	for i := 0; i < n; i++ {
		bm.Set(rng.Uint64() % universe)
	}
	return bm
}

func TestInitFromBufferUnlimited(t *testing.T) {
	src := randomBitmap(1, 10_000, 1_000_000)
	serialized := src.ToBufferWithCopy()

	t.Run("equivalent to FromBufferUnlimited", func(t *testing.T) {
		buf := make([]byte, len(serialized), len(serialized)+1024)
		copy(buf, serialized)

		var dst Bitmap
		InitFromBufferUnlimited(&dst, buf)
		require.Equal(t, FromBufferUnlimited(buf).ToArray(), dst.ToArray())
	})

	t.Run("reusing one struct across buffers", func(t *testing.T) {
		a := randomBitmap(2, 100, 10_000)
		b := randomBitmap(3, 200, 10_000)

		var dst Bitmap
		InitFromBufferUnlimited(&dst, a.ToBufferWithCopy())
		require.Equal(t, a.ToArray(), dst.ToArray())

		InitFromBufferUnlimited(&dst, b.ToBufferWithCopy())
		require.Equal(t, b.ToArray(), dst.ToArray())
	})

	t.Run("mutable within buffer capacity", func(t *testing.T) {
		buf := make([]byte, len(serialized), len(serialized)*2)
		copy(buf, serialized)

		var dst Bitmap
		InitFromBufferUnlimited(&dst, buf)
		require.True(t, dst.Set(999_999_999))
		require.True(t, dst.Contains(999_999_999))
	})

	t.Run("reinit drops the previous buffer", func(t *testing.T) {
		bufA := randomBitmap(7, 100, 10_000).ToBufferWithCopy()
		bufB := randomBitmap(8, 100, 10_000).ToBufferWithCopy()

		var dst Bitmap
		InitFromBufferUnlimited(&dst, bufA)
		require.Same(t, &bufA[0], &dst._ptr[0])

		InitFromBufferUnlimited(&dst, bufB)
		require.Same(t, &bufB[0], &dst._ptr[0])
	})

	t.Run("tiny buffer falls back to empty bitmap", func(t *testing.T) {
		var dst Bitmap
		InitFromBufferUnlimited(&dst, make([]byte, 0))
		require.True(t, dst.IsEmpty())
		require.True(t, dst.Set(42), "fallback bitmap must be mutable")
	})

	t.Run("tiny buffer fallback allocates only the backing slice", func(t *testing.T) {
		var dst Bitmap
		allocs := testing.AllocsPerRun(100, func() {
			InitFromBufferUnlimited(&dst, make([]byte, 0))
		})
		require.Equal(t, 1.0, allocs)
	})
}

func TestInitCloneToBuf(t *testing.T) {
	src := randomBitmap(4, 10_000, 1_000_000)

	t.Run("equivalent to CloneToBuf", func(t *testing.T) {
		buf := make([]byte, 0, src.LenInBytes()+1024)

		var dst Bitmap
		src.InitCloneToBuf(&dst, buf)
		require.Equal(t, src.ToArray(), dst.ToArray())
		require.Equal(t, src.CloneToBuf(make([]byte, 0, src.LenInBytes())).ToArray(), dst.ToArray())
	})

	t.Run("clone is independent of src", func(t *testing.T) {
		var dst Bitmap
		src.InitCloneToBuf(&dst, make([]byte, 0, src.LenInBytes()+1024))
		dst.Set(999_999_998)
		require.True(t, dst.Contains(999_999_998))
		require.False(t, src.Contains(999_999_998))
	})

	t.Run("reusing one struct across clones", func(t *testing.T) {
		a := randomBitmap(5, 100, 10_000)
		b := randomBitmap(6, 200, 10_000)
		buf := make([]byte, 0, max(a.LenInBytes(), b.LenInBytes()))

		var dst Bitmap
		a.InitCloneToBuf(&dst, buf)
		require.Equal(t, a.ToArray(), dst.ToArray())

		b.InitCloneToBuf(&dst, buf)
		require.Equal(t, b.ToArray(), dst.ToArray())
	})

	t.Run("reinit drops the previous buffer", func(t *testing.T) {
		a := randomBitmap(9, 100, 10_000)
		bufA := make([]byte, 0, a.LenInBytes())
		bufB := make([]byte, 0, a.LenInBytes())

		var dst Bitmap
		a.InitCloneToBuf(&dst, bufA)
		require.Same(t, &bufA[:1][0], &dst._ptr[0])

		a.InitCloneToBuf(&dst, bufB)
		require.Same(t, &bufB[:1][0], &dst._ptr[0])
	})

	t.Run("nil src initializes empty bitmap over buf", func(t *testing.T) {
		var nilSrc *Bitmap
		var dst Bitmap
		nilSrc.InitCloneToBuf(&dst, make([]byte, 0, 4096))
		require.True(t, dst.IsEmpty())
		require.True(t, dst.Set(7))
		require.True(t, dst.Contains(7))
	})

	t.Run("zero-value src initializes empty bitmap over buf", func(t *testing.T) {
		var src, dst Bitmap
		src.InitCloneToBuf(&dst, make([]byte, 0, 4096))
		require.True(t, dst.IsEmpty())
		require.True(t, dst.Set(7))
		require.True(t, dst.Contains(7))
	})

	t.Run("nil src with tiny buffer allocates only the backing slice", func(t *testing.T) {
		var nilSrc *Bitmap
		var dst Bitmap
		allocs := testing.AllocsPerRun(100, func() {
			nilSrc.InitCloneToBuf(&dst, make([]byte, 0))
		})
		require.Equal(t, 1.0, allocs)
	})

	t.Run("too-small buffer panics", func(t *testing.T) {
		var dst Bitmap
		require.Panics(t, func() {
			src.InitCloneToBuf(&dst, make([]byte, 0, 8))
		})
	})
}
