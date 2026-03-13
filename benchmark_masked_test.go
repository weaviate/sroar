package sroar

import (
	"testing"
)

// buildMaskedBitmap creates a bitmap with numKeys distinct keys, each holding
// valsPerKey values in its container. Keys are spread across positions so that
// applying mask collapses them into numUniqueAfterMask groups.
func buildMaskedBitmap(numKeys, valsPerKey, numUniqueAfterMask int) (*Bitmap, uint64) {
	bm := NewBitmap()

	// Use top 16 bits (48-63) to differentiate keys, mask will zero them
	// to cause collisions. Middle bits (16-47) cycle through numUniqueAfterMask
	// groups.
	//
	// mask keeps bits 16-47 and zeroes bits 48-63, causing keys with
	// the same middle bits to collapse.
	var mask uint64 = 0x0000FFFFFFFF0000

	for i := 0; i < numKeys; i++ {
		group := uint64(i % numUniqueAfterMask)
		pos := uint64(i / numUniqueAfterMask)
		key := (pos << 48) | (group << 16)
		for v := 0; v < valsPerKey; v++ {
			bm.Set(key | uint64(v))
		}
	}
	return bm, mask
}

// Benchmark scenarios:
//   - NoCollision: each key maps to a unique masked key (no merges)
//   - LowCollision: ~2 keys per masked group
//   - HighCollision: ~10 keys per masked group
//   - MassiveCollision: all keys collapse to 1 masked group
//
// Each scenario is tested with sparse (few values per container) and
// dense (many values per container) variants.

type maskedBenchCase struct {
	name               string
	numKeys            int
	valsPerKey         int
	numUniqueAfterMask int
}

var maskedBenchCases = []maskedBenchCase{
	// No collisions — pure copy path
	{"NoCollision_Sparse", 100, 10, 100},
	{"NoCollision_Dense", 100, 1000, 100},

	// Low collision — 2 keys per group
	{"LowCollision_Sparse", 100, 10, 50},
	{"LowCollision_Dense", 100, 1000, 50},

	// High collision — 10 keys per group
	{"HighCollision_Sparse", 100, 10, 10},
	{"HighCollision_Dense", 100, 1000, 10},

	// Massive collision — all keys collapse to 1
	{"MassiveCollision_Sparse", 100, 10, 1},
	{"MassiveCollision_Dense", 100, 1000, 1},

	// Large bitmap — 1000 keys, 10 per group
	{"Large_HighCollision_Sparse", 1000, 10, 100},
	{"Large_HighCollision_Dense", 1000, 1000, 100},
}

func BenchmarkMasked(b *testing.B) {
	for _, tc := range maskedBenchCases {
		bm, mask := buildMaskedBitmap(tc.numKeys, tc.valsPerKey, tc.numUniqueAfterMask)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bm.Masked(mask)
			}
		})
	}
}

func BenchmarkMaskedToBuf(b *testing.B) {
	for _, tc := range maskedBenchCases {
		bm, mask := buildMaskedBitmap(tc.numKeys, tc.valsPerKey, tc.numUniqueAfterMask)
		warm := bm.Masked(mask)
		buf := make([]byte, len(warm.data)*2+4096)

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bm.MaskedToBuf(mask, buf)
			}
		})
	}
}
