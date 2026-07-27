package sroar

import (
	"math/rand"
	"sort"
	"testing"
)

// Benchmarks comparing N-way union strategies across different source shapes:
//
//   - tiny sources (cardinality 1) — many singleton bitmaps, the shape that
//     makes pairwise merge slow
//   - mixed (tiny sources plus a few large ones)
//   - few large sources
//
// Strategies:
//
//   - IterOr:     acc.Or(src) one source at a time
//   - FastOr:     sroar's variadic two-pass union
//   - SortedList: extract all elements, sort, dedup, FromSortedList
//   - Accumulator: dense staging
func BenchmarkOr(b *testing.B) {
	makeTiny := func(rng *rand.Rand, n int, universe uint64) []*Bitmap {
		sources := make([]*Bitmap, n)
		for i := range sources {
			bm := NewBitmap()
			bm.Set(rng.Uint64() % universe)
			sources[i] = bm
		}
		return sources
	}
	makeLarge := func(seed int64, n, card int, universe uint64) []*Bitmap {
		sources := make([]*Bitmap, n)
		for i := range sources {
			sources[i] = randomBitmap(seed+int64(i), card, universe)
		}
		return sources
	}

	fixtures := []struct {
		name    string
		sources []*Bitmap
	}{
		{"tiny_1k_u300k", makeTiny(rand.New(rand.NewSource(1)), 1_000, 300_000)},
		{"tiny_10k_u300k", makeTiny(rand.New(rand.NewSource(2)), 10_000, 300_000)},
		{"tiny_100k_u300k", makeTiny(rand.New(rand.NewSource(3)), 100_000, 300_000)},
		{"tiny_100k_u10m", makeTiny(rand.New(rand.NewSource(4)), 100_000, 10_000_000)},
		{"tiny_100k_u100m", makeTiny(rand.New(rand.NewSource(8)), 100_000, 100_000_000)},
		{"tiny_100k_u300m", makeTiny(rand.New(rand.NewSource(9)), 100_000, 300_000_000)},
		{"tiny_100k_u1b", makeTiny(rand.New(rand.NewSource(10)), 100_000, 1_000_000_000)},
		{"mixed_10k_tiny_5_large_u10m", append(
			makeTiny(rand.New(rand.NewSource(5)), 10_000, 10_000_000),
			makeLarge(6, 5, 200_000, 10_000_000)...)},
		{"large_8x1m_u10m", makeLarge(7, 8, 1_000_000, 10_000_000)},
	}

	strategies := []struct {
		name  string
		union func(sources []*Bitmap) *Bitmap
	}{
		{"IterOr", func(sources []*Bitmap) *Bitmap {
			acc := NewBitmap()
			for _, s := range sources {
				acc.Or(s)
			}
			return acc
		}},
		{"FastOr", func(sources []*Bitmap) *Bitmap {
			return FastOr(sources...)
		}},
		{"SortedList", func(sources []*Bitmap) *Bitmap {
			var vals []uint64
			for _, s := range sources {
				vals = append(vals, s.ToArray()...)
			}
			sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
			// Dedup in place; FromSortedList requires strictly deduped input.
			n := 0
			for i, v := range vals {
				if i == 0 || v != vals[n-1] {
					vals[n] = v
					n++
				}
			}
			return FromSortedList(vals[:n])
		}},
		{"Accumulator", func(sources []*Bitmap) *Bitmap {
			acc := NewAccumulator()
			for _, s := range sources {
				acc.Or(s)
			}
			return acc.Bitmap()
		}},
	}

	for _, fx := range fixtures {
		// Sanity: all strategies must agree before their numbers mean anything.
		want := strategies[0].union(fx.sources).GetCardinality()
		for _, st := range strategies[1:] {
			if got := st.union(fx.sources).GetCardinality(); got != want {
				b.Fatalf("%s/%s: cardinality %d, want %d", fx.name, st.name, got, want)
			}
		}
		for _, st := range strategies {
			b.Run(fx.name+"/"+st.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = st.union(fx.sources)
				}
			})
		}
	}
}
