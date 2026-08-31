package sroar

import (
	"math/rand"
	"testing"
)

// go test -v -bench BenchmarkPrefillNative -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func BenchmarkPrefillNative(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Prefill(200_000_000)
	}
}

// go test -v -bench BenchmarkPrefillFromSortedList -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func BenchmarkPrefillFromSortedList(b *testing.B) {
	prefillBufferSize := 65_536
	maxVal := uint64(200_000_000)
	inc := uint64(prefillBufferSize)
	buf := make([]uint64, prefillBufferSize)

	for i := 0; i < b.N; i++ {
		finalBM := NewBitmap()

		for i := uint64(0); i <= maxVal; i += inc {
			j := uint64(0)
			for ; j < inc && i+j <= maxVal; j++ {
				buf[j] = i + j
			}
			finalBM.Or(FromSortedList(buf[:j]))
		}
	}
}

// go test -v -bench BenchmarkFillUpNative -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func BenchmarkFillUpNative(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bm := Prefill(100_000_000)
		bm.FillUp(150_000_000)
		bm.FillUp(200_000_000)
	}
}

// go test -v -bench BenchmarkPrefillFromSortedList -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func BenchmarkFillUpFromSortedList(b *testing.B) {
	prefillBufferSize := 65_536
	prefillX := uint64(100_000_000)
	fillupX1 := uint64(150_000_000)
	fillupX2 := uint64(200_000_000)
	inc := uint64(prefillBufferSize)
	buf := make([]uint64, prefillBufferSize)

	for i := 0; i < b.N; i++ {
		bm := Prefill(prefillX)

		for i := prefillX + 1; i <= fillupX1; i += inc {
			j := uint64(0)
			for ; j < inc && i+j <= fillupX1; j++ {
				buf[j] = i + j
			}
			bm.Or(FromSortedList(buf[:j]))
		}
		for i := fillupX1 + 1; i <= fillupX2; i += inc {
			j := uint64(0)
			for ; j < inc && i+j <= fillupX2; j++ {
				buf[j] = i + j
			}
			bm.Or(FromSortedList(buf[:j]))
		}
	}
}

// ================================================================================
//
// BENCHMARKS comparing performance of different merge implementations
//
// dataset generated inside init() method to be shared between all benchmarks
// (commented at the moment, not to be called with remaining normal tests)
//
// results of benchmarks run on:
//	goos: darwin
//	goarch: arm64
//	pkg: github.com/weaviate/sroar
//	cpu: Apple M1 Pro
// included below
// (countSubsets=10, countElements=7, 67, 567, 4567, 34567, 234567, 1234567)
//
// ================================================================================

var superset *Bitmap
var bigset *Bitmap
var subsets []*Bitmap
var bufs10 [][]uint16

// func init() {
// 	initMerge()
// }

func initMerge() {
	randSeed := int64(1724861525311)
	// randSeed := time.Now().UnixNano()
	countSubsets := 10
	countElements := 123456789
	// countElements := 720000 // 33 cont
	// countElements := 786432 // 36 cont
	// countElements := 1048576 // 48 cont
	// countElements := 3456789 // 159 cont

	containers := (countElements + maxCardinality - 1) / maxCardinality
	maxX := 3 * containers * maxCardinality
	rnd := rand.New(rand.NewSource(randSeed))

	superset = NewBitmap()
	for i := 0; i < countElements; i++ {
		x := uint64(rnd.Intn(maxX))
		superset.Set(x)
	}
	bigset = superset.Clone()

	subsets = make([]*Bitmap, countSubsets)
	for i := range subsets {
		subsets[i] = NewBitmap()
		// each next subset bitmap contains fewer elements
		// 1/2 of countElements, 1/3, 1/4, ...
		for j, c := 0, countElements/(i+2); j < c; j++ {
			x := uint64(rnd.Intn(maxX))
			subsets[i].Set(x)
			// ensure superset contains element of subset
			superset.Set(x)
		}
	}

	bufs10 = make([][]uint16, 10)
	for i := range bufs10 {
		bufs10[i] = make([]uint16, maxContainerSize)
	}
}

// go test -v -bench Benchmark_And_Old -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_And_Old(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.AndOld(subsets[j])
			s2.AndOld(subsets[l-j-1])
			b1.AndOld(subsets[j])
			b2.AndOld(subsets[l-j-1])
		}
	}
}

// go test -v -bench Benchmark_And_OldFn -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_And_OldFn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1 = AndOld(s1, subsets[j])
			s2 = AndOld(s2, subsets[l-j-1])
			b1 = AndOld(b1, subsets[j])
			b2 = AndOld(b2, subsets[l-j-1])
		}
	}
}

// go test -v -bench Benchmark_And_Alt -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_And_Alt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.And(subsets[j])
			s2.And(subsets[l-j-1])
			b1.And(subsets[j])
			b2.And(subsets[l-j-1])
		}
	}
}

// go test -v -bench Benchmark_And_AltFn -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_And_AltFn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1 = And(s1, subsets[j])
			s2 = And(s2, subsets[l-j-1])
			b1 = And(b1, subsets[j])
			b2 = And(b2, subsets[l-j-1])
		}
	}
}

func Benchmark_And_Alt_Conc_0(b *testing.B) {
	benchmark_And_Conc(b, 0)
}

func Benchmark_And_Alt_Conc_1(b *testing.B) {
	benchmark_And_Conc(b, 1)
}

func Benchmark_And_Alt_Conc_2(b *testing.B) {
	benchmark_And_Conc(b, 2)
}

func Benchmark_And_Alt_Conc_3(b *testing.B) {
	benchmark_And_Conc(b, 3)
}

func Benchmark_And_Alt_Conc_4(b *testing.B) {
	benchmark_And_Conc(b, 4)
}

func Benchmark_And_Alt_Conc_5(b *testing.B) {
	benchmark_And_Conc(b, 5)
}

func Benchmark_And_Alt_Conc_6(b *testing.B) {
	benchmark_And_Conc(b, 6)
}

func Benchmark_And_Alt_Conc_7(b *testing.B) {
	benchmark_And_Conc(b, 7)
}

func Benchmark_And_Alt_Conc_8(b *testing.B) {
	benchmark_And_Conc(b, 8)
}

func Benchmark_And_Alt_Conc_9(b *testing.B) {
	benchmark_And_Conc(b, 9)
}

func Benchmark_And_Alt_Conc_10(b *testing.B) {
	benchmark_And_Conc(b, 10)
}

func benchmark_And_Conc(b *testing.B, concurrency int) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.AndConc(subsets[j], concurrency)
			s2.AndConc(subsets[l-j-1], concurrency)
			b1.AndConc(subsets[j], concurrency)
			b2.AndConc(subsets[l-j-1], concurrency)
		}
	}
}

// go test -v -bench Benchmark_AndNot_Old -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_AndNot_Old(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.AndNotOld(subsets[j])
			s2.AndNotOld(subsets[l-j-1])
			b1.AndNotOld(subsets[j])
			b2.AndNotOld(subsets[l-j-1])
		}
	}
}

// go test -v -bench Benchmark_AndNot_Alt -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_AndNot_Alt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.AndNot(subsets[j])
			s2.AndNot(subsets[l-j-1])
			b1.AndNot(subsets[j])
			b2.AndNot(subsets[l-j-1])
		}
	}
}

// go test -v -bench Benchmark_AndNot_AltFn -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_AndNot_AltFn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1 = AndNot(s1, subsets[j])
			s2 = AndNot(s2, subsets[l-j-1])
			b1 = AndNot(b1, subsets[j])
			b2 = AndNot(b2, subsets[l-j-1])
		}
	}
}

func Benchmark_AndNot_Alt_Conc_0(b *testing.B) {
	benchmark_AndNot_Conc(b, 0)
}

func Benchmark_AndNot_Alt_Conc_1(b *testing.B) {
	benchmark_AndNot_Conc(b, 1)
}

func Benchmark_AndNot_Alt_Conc_2(b *testing.B) {
	benchmark_AndNot_Conc(b, 2)
}

func Benchmark_AndNot_Alt_Conc_3(b *testing.B) {
	benchmark_AndNot_Conc(b, 3)
}

func Benchmark_AndNot_Alt_Conc_4(b *testing.B) {
	benchmark_AndNot_Conc(b, 4)
}

func Benchmark_AndNot_Alt_Conc_5(b *testing.B) {
	benchmark_AndNot_Conc(b, 5)
}

func Benchmark_AndNot_Alt_Conc_6(b *testing.B) {
	benchmark_AndNot_Conc(b, 6)
}

func Benchmark_AndNot_Alt_Conc_7(b *testing.B) {
	benchmark_AndNot_Conc(b, 7)
}

func Benchmark_AndNot_Alt_Conc_8(b *testing.B) {
	benchmark_AndNot_Conc(b, 8)
}

func Benchmark_AndNot_Alt_Conc_9(b *testing.B) {
	benchmark_AndNot_Conc(b, 9)
}

func Benchmark_AndNot_Alt_Conc_10(b *testing.B) {
	benchmark_AndNot_Conc(b, 10)
}

func benchmark_AndNot_Conc(b *testing.B, concurrency int) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.AndNotConc(subsets[j], concurrency)
			s2.AndNotConc(subsets[l-j-1], concurrency)
			b1.AndNotConc(subsets[j], concurrency)
			b2.AndNotConc(subsets[l-j-1], concurrency)
		}
	}
}

// go test -v -bench Benchmark_Or_Old -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_Or_Old(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.OrOld(subsets[j])
			s2.OrOld(subsets[l-j-1])
			b1.OrOld(subsets[j])
			b2.OrOld(subsets[l-j-1])
		}
	}
}

// go test -v -bench Benchmark_Or_OldFn -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_Or_OldFn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1 = OrOld(s1, subsets[j])
			s2 = OrOld(s2, subsets[l-j-1])
			b1 = OrOld(b1, subsets[j])
			b2 = OrOld(b2, subsets[l-j-1])
		}
	}
}

// go test -v -bench Benchmark_Or_Alt -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_Or_Alt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.Or(subsets[j])
			s2.Or(subsets[l-j-1])
			b1.Or(subsets[j])
			b2.Or(subsets[l-j-1])
		}
	}
}

// go test -v -bench Benchmark_Or_AltFn -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_Or_AltFn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1 = Or(s1, subsets[j])
			s2 = Or(s2, subsets[l-j-1])
			b1 = Or(b1, subsets[j])
			b2 = Or(b2, subsets[l-j-1])
		}
	}
}

func Benchmark_Or_Alt_Conc_0(b *testing.B) {
	benchmark_Or_Conc(b, 0)
}

func Benchmark_Or_Alt_Conc_1(b *testing.B) {
	benchmark_Or_Conc(b, 1)
}

func Benchmark_Or_Alt_Conc_2(b *testing.B) {
	benchmark_Or_Conc(b, 2)
}

func Benchmark_Or_Alt_Conc_3(b *testing.B) {
	benchmark_Or_Conc(b, 3)
}

func Benchmark_Or_Alt_Conc_4(b *testing.B) {
	benchmark_Or_Conc(b, 4)
}

func Benchmark_Or_Alt_Conc_5(b *testing.B) {
	benchmark_Or_Conc(b, 5)
}

func Benchmark_Or_Alt_Conc_6(b *testing.B) {
	benchmark_Or_Conc(b, 6)
}

func Benchmark_Or_Alt_Conc_7(b *testing.B) {
	benchmark_Or_Conc(b, 7)
}

func Benchmark_Or_Alt_Conc_8(b *testing.B) {
	benchmark_Or_Conc(b, 8)
}

func Benchmark_Or_Alt_Conc_9(b *testing.B) {
	benchmark_Or_Conc(b, 9)
}

func Benchmark_Or_Alt_Conc_10(b *testing.B) {
	benchmark_Or_Conc(b, 10)
}

func benchmark_Or_Conc(b *testing.B, concurrency int) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.OrConc(subsets[j], concurrency)
			s2.OrConc(subsets[l-j-1], concurrency)
			b1.OrConc(subsets[j], concurrency)
			b2.OrConc(subsets[l-j-1], concurrency)
		}
	}
}

// go test -v -bench BenchmarkFromSortedList -benchmem -run ^$ github.com/weaviate/sroar
func BenchmarkFromSortedList(b *testing.B) {
	// Containers are sized for distinct*reps, so the fill has to shrink or
	// rewrite them — the path these cases measure.
	genDupSeg := func(keys, distinct, reps int) []uint64 {
		vals := make([]uint64, 0, keys*distinct*reps)
		for k := 0; k < keys; k++ {
			base := uint64(k) << 16
			for i := 0; i < distinct; i++ {
				for r := 0; r < reps; r++ {
					vals = append(vals, base+uint64(i))
				}
			}
		}
		return vals
	}

	// newGetBuf hands each sub-benchmark its own buffer, grown once and
	// reused across iterations so the build is measured, not the allocation.
	newGetBuf := func() func(sizeBytes int) []byte {
		var buf []byte
		return func(sizeBytes int) []byte {
			if cap(buf) < sizeBytes {
				buf = make([]byte, sizeBytes)
			}
			return buf[:sizeBytes]
		}
	}

	for _, bc := range []struct {
		name string
		vals []uint64
	}{
		{"dense_1M", sortedSeq(1<<20, 1)},
		{"dense_100k", sortedSeq(100_000, 1)},
		{"dense_10k", sortedSeq(10_000, 1)},
		{"dense_1k", sortedSeq(1_000, 1)},
		{"sparse_100k", sortedSeq(100_000, 1000)},
		{"sparse_10k", sortedSeq(10_000, 1000)},
		{"verysparse_10k", sortedSeq(10_000, 1<<16)},
		// sized as a bitmap, collapses to an array
		{"dup_collapse_40k", genDupSeg(10, 1_000, 4)},
		// sized as an array, truncated
		{"dup_array_10k", genDupSeg(10, 500, 2)},
		// stays a bitmap
		{"dup_bitmap_100k", genDupSeg(10, 5_000, 2)},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				FromSortedList(bc.vals)
			}
		})
		b.Run(bc.name+"/ToBuf", func(b *testing.B) {
			getBuf := newGetBuf()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				FromSortedListToBuf(bc.vals, getBuf)
			}
		})

		// Every case above fits in 32 bits, so the 32 variants do the same
		// work over half the input bytes.
		vals32 := narrow(b, bc.vals)
		b.Run(bc.name+"/32", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				FromSortedList32(vals32)
			}
		})
		b.Run(bc.name+"/32/ToBuf", func(b *testing.B) {
			getBuf := newGetBuf()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				FromSortedList32ToBuf(vals32, getBuf)
			}
		})
	}
}
