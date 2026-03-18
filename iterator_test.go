/*
 * Copyright 2021 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sroar

import (
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIteratorBasic(t *testing.T) {
	n := uint64(1e5)
	bm := NewBitmap()
	for i := uint64(1); i <= n; i++ {
		bm.Set(uint64(i))
	}

	it := bm.NewIterator()
	for i := uint64(1); i <= n; i++ {
		v := it.Next()
		require.Equal(t, i, v)
	}
	v := it.Next()
	require.Equal(t, uint64(0), v)
}

func TestIteratorRanges(t *testing.T) {
	n := uint64(1e5)
	bm := NewBitmap()
	for i := uint64(1); i <= n; i++ {
		bm.Set(uint64(i))
	}

	iters := bm.NewRangeIterators(8)
	cnt := uint64(1)
	for idx := 0; idx < 8; idx++ {
		it := iters[idx]
		for v := it.Next(); v > 0; v = it.Next() {
			require.Equal(t, cnt, v)
			cnt++
		}
	}
}

func TestIteratorRandom(t *testing.T) {
	n := uint64(1e6)
	bm := NewBitmap()
	mp := make(map[uint64]struct{})
	var arr []uint64
	for i := uint64(1); i <= n; i++ {
		v := uint64(rand.Intn(int(n) * 5))
		if v == 0 {
			continue
		}
		if _, ok := mp[v]; ok {
			continue
		}
		mp[v] = struct{}{}
		arr = append(arr, v)
		bm.Set(uint64(v))
	}

	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})

	it := bm.NewIterator()
	v := it.Next()
	for i := uint64(0); i < uint64(len(arr)); i++ {
		require.Equal(t, arr[i], v)
		v = it.Next()
	}
}

func TestIteratorWithRemoveKeys(t *testing.T) {
	b := NewBitmap()
	N := uint64(1e6)
	for i := uint64(0); i < N; i++ {
		b.Set(i)
	}

	b.RemoveRange(0, N)
	it := b.NewIterator()

	cnt := 0
	for it.Next() > 0 {
		cnt++
	}
	require.Equal(t, 0, cnt)
}

func TestManyIterator(t *testing.T) {
	b := NewBitmap()
	for i := 0; i < int(1e6); i++ {
		b.Set(uint64(i))
	}

	mi := b.ManyIterator()
	buf := make([]uint64, 1000)

	i := 0
	for {
		got := mi.NextMany(buf)
		if got == 0 {
			break
		}
		require.Equal(t, 1000, got)
		require.Equal(t, uint64(i*1000), buf[0])
		i++
	}
}

func BenchmarkIterator(b *testing.B) {
	bm := NewBitmap()
	for i := 0; i < int(1e5); i++ {
		bm.Set(uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := bm.NewIterator()
		for it.Next() > 0 {
		}
	}
}

func TestIteratorBasicDims(t *testing.T) {
	dims := uint16(3)
	n := uint64(1e5)
	bm := NewBitmap()

	for x := uint64(1); x <= n; x++ {
		for dim := uint16(0); dim < dims; dim++ {
			bm.SetDim(x, dim)
		}
	}

	iterators := make([]*Iterator, dims)
	for dim := uint16(0); dim < dims; dim++ {
		iterators[dim] = bm.NewIteratorDim(dim)
	}

	for x := uint64(1); x <= n; x++ {
		for dim := uint16(0); dim < dims; dim++ {
			xx := iterators[dim].Next()
			require.Equal(t, x, xx)
		}
	}

	for dim := uint16(0); dim < dims; dim++ {
		xx := iterators[dim].Next()
		require.Equal(t, uint64(0), xx)
	}
}

func TestIteratorRandomDims(t *testing.T) {
	dims := uint16(3)
	n := uint64(1e6)
	bm := NewBitmap()
	mp := make(map[uint64]struct{})
	var arr []uint64
	for i := uint64(1); i <= n; i++ {
		x := uint64(rand.Intn(int(n) * 5))
		if x == 0 {
			continue
		}
		if _, ok := mp[x]; ok {
			continue
		}
		mp[x] = struct{}{}
		arr = append(arr, x)
		for dim := uint16(0); dim < dims; dim++ {
			bm.SetDim(x, dim)
		}
	}

	slices.Sort(arr)

	iterators := make([]*Iterator, dims)
	xx := make([]uint64, dims)
	for dim := uint16(0); dim < dims; dim++ {
		iterators[dim] = bm.NewIterator()
		xx[dim] = iterators[dim].Next()
	}
	for i := uint64(0); i < uint64(len(arr)); i++ {
		for dim := uint16(0); dim < dims; dim++ {
			require.Equal(t, arr[i], xx[dim])
			xx[dim] = iterators[dim].Next()
		}
	}
}

func TestIteratorDim(t *testing.T) {
	t.Run("empty bitmap", func(t *testing.T) {
		bm := NewBitmap()
		require.Equal(t, uint64(0), bm.NewIteratorDim(0).Next())
		require.Equal(t, uint64(0), bm.NewIteratorDim(5).Next())
	})

	t.Run("absent dim", func(t *testing.T) {
		// Bitmap has values for dims 0 and 1, but not 2.
		bm := NewBitmap()
		for x := uint64(1); x <= 100; x++ {
			bm.SetDim(x, 0)
			bm.SetDim(x, 1)
		}

		require.Equal(t, uint64(0), bm.NewIteratorDim(2).Next())
	})

	t.Run("single value", func(t *testing.T) {
		bm := NewBitmap()
		bm.SetDim(42, 3)

		it := bm.NewIteratorDim(3)
		require.Equal(t, uint64(42), it.Next())
		require.Equal(t, uint64(0), it.Next())
	})

	t.Run("exhaustion is idempotent", func(t *testing.T) {
		bm := NewBitmap()
		bm.SetDim(1, 0)
		bm.SetDim(2, 0)

		it := bm.NewIteratorDim(0)
		require.Equal(t, uint64(1), it.Next())
		require.Equal(t, uint64(2), it.Next())
		require.Equal(t, uint64(0), it.Next())
		require.Equal(t, uint64(0), it.Next())
		require.Equal(t, uint64(0), it.Next())
	})

	t.Run("large dim value (max uint16)", func(t *testing.T) {
		bm := NewBitmap()
		dim := uint16(65535)
		vals := []uint64{1, 100, 1000}
		for _, v := range vals {
			bm.SetDim(v, dim)
			bm.SetDim(v, 0) // noise in another dim to confirm filtering
		}

		it := bm.NewIteratorDim(dim)
		for _, exp := range vals {
			require.Equal(t, exp, it.Next())
		}
		require.Equal(t, uint64(0), it.Next())
	})

	t.Run("partial overlap between dims", func(t *testing.T) {
		// dim 0: 5, 10, 15, 20, 25, 30
		// dim 1: 7, 10, 17, 20, 27, 30
		bm := NewBitmap()
		for _, x := range []uint64{5, 15, 25} {
			bm.SetDim(x, 0)
		}
		for _, x := range []uint64{7, 17, 27} {
			bm.SetDim(x, 1)
		}
		for _, x := range []uint64{10, 20, 30} {
			bm.SetDim(x, 0)
			bm.SetDim(x, 1)
		}

		it0 := bm.NewIteratorDim(0)
		for _, exp := range []uint64{5, 10, 15, 20, 25, 30} {
			require.Equal(t, exp, it0.Next())
		}
		require.Equal(t, uint64(0), it0.Next())

		it1 := bm.NewIteratorDim(1)
		for _, exp := range []uint64{7, 10, 17, 20, 27, 30} {
			require.Equal(t, exp, it1.Next())
		}
		require.Equal(t, uint64(0), it1.Next())
	})

	t.Run("multiple containers", func(t *testing.T) {
		// Each container covers a 65536-wide range; values here cross several boundaries.
		bm := NewBitmap()
		dim := uint16(2)
		vals := []uint64{1, 65537, 131073, 196609}
		for _, v := range vals {
			bm.SetDim(v, dim)
			bm.SetDim(v, 0) // noise in another dim to confirm filtering
		}

		it := bm.NewIteratorDim(dim)
		for _, exp := range vals {
			require.Equal(t, exp, it.Next())
		}
		require.Equal(t, uint64(0), it.Next())
	})

	t.Run("disjoint value sets per dim", func(t *testing.T) {
		// dim 0: even numbers 2..20, dim 1: odd numbers 1..19
		bm := NewBitmap()
		for x := uint64(1); x <= 10; x++ {
			bm.SetDim(x*2, 0)
			bm.SetDim(x*2-1, 1)
		}

		it0 := bm.NewIteratorDim(0)
		for x := uint64(1); x <= 10; x++ {
			require.Equal(t, x*2, it0.Next())
		}
		require.Equal(t, uint64(0), it0.Next())

		it1 := bm.NewIteratorDim(1)
		for x := uint64(1); x <= 10; x++ {
			require.Equal(t, x*2-1, it1.Next())
		}
		require.Equal(t, uint64(0), it1.Next())
	})

	t.Run("sorted order regardless of insertion order", func(t *testing.T) {
		bm := NewBitmap()
		for _, v := range []uint64{500, 300, 100, 400, 200} {
			bm.SetDim(v, 1)
		}

		it := bm.NewIteratorDim(1)
		for _, exp := range []uint64{100, 200, 300, 400, 500} {
			require.Equal(t, exp, it.Next())
		}
		require.Equal(t, uint64(0), it.Next())
	})
}

func TestIteratorWithRemoveKeysDims(t *testing.T) {
	dims := uint16(3)
	b := NewBitmap()
	N := uint64(1e6)
	for x := uint64(0); x < N; x++ {
		for dim := uint16(0); dim < dims; dim++ {
			b.SetDim(x, dim)
		}
	}

	for dim := uint16(0); dim < dims; dim++ {
		b.RemoveRangeDim(0, N, dim)
		it := b.NewIteratorDim(dim)

		cnt := 0
		for it.Next() > 0 {
			cnt++
		}
		require.Equal(t, 0, cnt)
	}
}
