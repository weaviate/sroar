package sroar

import (
	"math/rand"
	"testing"
)

// buildFloorMaskedInputs constructs vals and candidates bitmaps with values
// encoded as (aux << 36) | doc, with aux in bits 36..63 and doc in bits 0..35.
// mask = 0x0000000FFFFFFFFF (mask_low16 = 0xFFFF) groups values by doc.
//
// This shape — high bits encode an auxiliary identifier the mask strips,
// low bits encode the grouping key — exercises the non-contiguous-high
// mask path that FloorMasked is designed for.
//
// numAux controls how many aux IDs share each doc (= how often the
// membership lookup finds a non-trivial match path); numDocs is the
// distinct-doc count (= number of masked groups). totalBits is the
// approximate total bit count per bitmap; bits are spread roughly evenly
// across (aux, doc) pairs.
func buildFloorMaskedInputs(seed int64, numAux, numDocs, totalBits int) (vals, candidates *Bitmap, mask uint64) {
	mask = 0x0000000FFFFFFFFF
	rnd := rand.New(rand.NewSource(seed))
	vals = NewBitmap()
	candidates = NewBitmap()
	for i := 0; i < totalBits; i++ {
		aux := uint64(rnd.Intn(numAux))
		doc := uint64(rnd.Intn(numDocs))
		vals.Set((aux << 36) | doc)
	}
	for i := 0; i < totalBits; i++ {
		aux := uint64(rnd.Intn(numAux))
		doc := uint64(rnd.Intn(numDocs))
		candidates.Set((aux << 36) | doc)
	}
	return vals, candidates, mask
}

type floorBenchCase struct {
	name      string
	numAux    int
	numDocs   int
	totalBits int
}

var floorBenchCases = []floorBenchCase{
	// Typical scale: 1M bits, ~5 aux levels, 200K groups.
	{"Realistic", 5, 200_000, 1_000_000},

	// Dense per group: fewer groups, more bits each.
	{"Dense", 10, 10_000, 1_000_000},

	// Sparse per group: many groups, few bits per group.
	{"Sparse", 5, 2_000_000, 500_000},

	// Small: tip of the small-input regime.
	{"Small", 4, 100, 1_000},
}

func BenchmarkFloorMasked(b *testing.B) {
	for _, tc := range floorBenchCases {
		vals, candidates, mask := buildFloorMaskedInputs(20260622, tc.numAux, tc.numDocs, tc.totalBits)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				FloorMasked(vals, candidates, mask)
			}
		})
	}
}

func BenchmarkFloorMaskedToBuf(b *testing.B) {
	for _, tc := range floorBenchCases {
		vals, candidates, mask := buildFloorMaskedInputs(20260622, tc.numAux, tc.numDocs, tc.totalBits)
		// Size buf upper-bounded by candidates' storage — the result can
		// never exceed candidates in container count or bits.
		warm := FloorMasked(vals, candidates, mask)
		buf := make([]byte, len(warm.data)*2+4096)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				FloorMaskedToBuf(vals, candidates, mask, buf)
			}
		})
	}
}
