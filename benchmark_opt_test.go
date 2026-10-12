package sroar

import (
	"math/rand"
	"slices"
	"strconv"
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

// BenchmarkCompacted measures compaction against OR-ing into an empty bitmap,
// which reclaims dead space but never shrinks a container.
func BenchmarkCompacted(b *testing.B) {
	orIntoEmpty := func(bm *Bitmap) *Bitmap {
		res := NewBitmap()
		res.Or(bm)
		return res
	}

	// sparseArrays: nothing to shrink, so only the surrounding work differs.
	sparseArrays := func(nContainers int) *Bitmap {
		bm := NewBitmap()
		for k := 0; k < nContainers; k++ {
			for i := 0; i < 5; i++ {
				bm.Set(uint64(k)<<16 | uint64(i*7))
			}
		}
		return bm
	}
	arraysWithSlack := func(nContainers int) *Bitmap {
		bm := NewBitmap()
		for k := 0; k < nContainers; k++ {
			for i := 0; i < 600; i++ {
				bm.Set(uint64(k)<<16 | uint64(i))
			}
			for i := 20; i < 600; i++ {
				bm.Remove(uint64(k)<<16 | uint64(i))
			}
		}
		return bm
	}
	// sparseBitmaps: the one shape OR-ing into an empty bitmap cannot fix.
	sparseBitmaps := func(nContainers int) *Bitmap {
		bm := Prefill(uint64(nContainers)<<16 - 1)
		for k := 0; k < nContainers; k++ {
			bm.RemoveRange(uint64(k)<<16|500, uint64(k+1)<<16)
		}
		return bm
	}

	for _, bc := range []struct {
		name  string
		build func() *Bitmap
	}{
		{"sparse_arrays_100", func() *Bitmap { return sparseArrays(100) }},
		{"sparse_arrays_10k", func() *Bitmap { return sparseArrays(10_000) }},
		{"arrays_with_slack_100", func() *Bitmap { return arraysWithSlack(100) }},
		{"sparse_bitmaps_100", func() *Bitmap { return sparseBitmaps(100) }},
		{"dense_1M", func() *Bitmap { return Prefill(1 << 20) }},
	} {
		b.Run(bc.name, func(b *testing.B) {
			bm := bc.build()
			b.Run("OrIntoEmpty", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					orIntoEmpty(bm)
				}
			})
			b.Run("Compacted", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					bm.Compacted()
				}
			})
		})
	}
}

// BenchmarkCompactedFirstInsert measures the first-insert cost Compacted's
// godoc names. per is the values per container: 56 and 601 size the copy with
// a spare slot, 60 and 600 without; only 600 leaves slack in the source.
func BenchmarkCompactedFirstInsert(b *testing.B) {
	const nContainers = 100
	source := func(per int) *Bitmap {
		bm := NewBitmap()
		for k := 0; k < nContainers; k++ {
			for i := 0; i < per; i++ {
				bm.Set(uint64(k)<<16 | uint64(i))
			}
		}
		return bm
	}
	insertOne := func(bm *Bitmap) {
		for k := 0; k < nContainers; k++ {
			bm.Set(uint64(k)<<16 | 60000)
		}
	}

	for _, per := range []int{56, 60, 600, 601} {
		src := source(per)
		b.Run("per="+strconv.Itoa(per), func(b *testing.B) {
			b.Run("Compacted", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					cp := src.Compacted()
					b.StartTimer()
					insertOne(cp)
				}
			})
			b.Run("OrIntoEmpty", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					cp := NewBitmap()
					cp.Or(src)
					b.StartTimer()
					insertOne(cp)
				}
			})
		})
	}
}

// go test -bench BenchmarkGrownCap -run ^$ github.com/weaviate/sroar
func BenchmarkGrownCap(b *testing.B) {
	// The trace appends one sparse container and its key entry at a time, so peak
	// slack and copy work can be read off against the pair that produced them.
	const step = minContainerSize + 8

	for _, limit := range []int{512 * 1024, growDoubleLimit, 4 * growDoubleLimit} {
		for _, div := range []int{2, growSlackDiv, 8} {
			for _, finalSize := range []int{1 << 22, 1 << 24, 1 << 25} {
				if finalSize < 2*limit {
					// The trace never leaves the doubling arm, so the pair is unswept.
					continue
				}
				name := "limit_" + strconv.Itoa(limit) + "/div_" + strconv.Itoa(div) +
					"/final_" + strconv.Itoa(finalSize)
				b.Run(name, func(b *testing.B) {
					var ln, copied, peakSlack int
					buf := []uint16(nil)
					for i := 0; i < b.N; i++ {
						ln, copied, peakSlack = 0, 0, 0
						buf = nil
						for ln < finalSize {
							if ln+step > cap(buf) {
								grown := make([]uint16, ln, grownCapWith(cap(buf), step, limit, div))
								copied += copy(grown, buf)
								buf = grown
								if s := cap(buf) - (ln + step); s > peakSlack {
									peakSlack = s
								}
							}
							ln += step
							buf = buf[:ln]
						}
					}
					b.ReportMetric(float64(peakSlack)*2, "peak-slack-bytes")
					b.ReportMetric(float64(copied)*2, "bytes-copied")
				})
			}
		}
	}
}

// go test -v -bench BenchmarkOrArrayInPlace -benchmem -run ^$ github.com/weaviate/sroar
func BenchmarkOrArrayInPlace(b *testing.B) {
	sizes := []struct{ cnum, onum int }{
		{16, 16}, {16, 512}, {512, 16}, {128, 128}, {512, 512}, {64, 1200}, {1200, 64}, {1200, 1200},
	}
	for _, sz := range sizes {
		for _, overlapPct := range []int{0, 50, 100} {
			cv, ov := orArrayBenchValues(sz.cnum, sz.onum, overlapPct)
			src := arrayContainer(int(startIdx)+sz.cnum+sz.onum, cv)
			other := arrayContainer(int(startIdx)+sz.onum, ov)
			c := make([]uint16, len(src))
			buf := make([]uint16, maxContainerSize)

			name := "c" + strconv.Itoa(sz.cnum) + "_o" + strconv.Itoa(sz.onum) + "_overlap" + strconv.Itoa(overlapPct)
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					copy(c, src)
					containerOrAlt(c, other, buf, runInline)
				}
			})
		}
	}
}

// orArrayBenchValues returns c's values (even numbers) and other's, which share
// overlapPct% of the smaller side with c and fill the rest with odd values.
func orArrayBenchValues(cnum, onum, overlapPct int) (cv, ov []uint16) {
	cv = uint16Range(0, 2*cnum, 2)
	shared := min(cnum, onum) * overlapPct / 100
	ov = uint16Range(1, 2*(onum-shared), 2)
	for i := 0; i < shared; i++ {
		ov = append(ov, cv[i*cnum/shared])
	}
	slices.Sort(ov)
	return cv, ov
}

// BenchmarkOrArrayRoute compares union2by2, stored as orArrayAlt stores it,
// against orArrayAsBitmap converted back to an array when the union is
// array-sized. Merging wins whenever the union is array-sized.
//
// go test -v -bench BenchmarkOrArrayRoute -benchmem -run ^$ github.com/weaviate/sroar
func BenchmarkOrArrayRoute(b *testing.B) {
	vals := make([]uint16, maxContainerSize)
	routes := []struct {
		name string
		or   func(c, other array, buf []uint16, runMode int)
	}{
		{"merge", func(c, other array, buf []uint16, runMode int) {
			num := union2by2(c.all(), other.all(), buf[startIdx:])
			switch {
			case runMode&runInline != 0 && c.canFitArray(num):
				copy(c[startIdx:], buf[startIdx:int(startIdx)+num])
			case !isArrayCard(num):
				c.orArrayAsBitmap(other, buf, runMode)
			}
		}},
		{"bitmap", func(c, other array, buf []uint16, runMode int) {
			out := c.orArrayAsBitmap(other, buf, runMode)
			if out != nil && isArrayCard(getCardinality(out)) {
				bitmap(out).intoArray(vals)
			}
		}},
	}
	modes := []struct {
		name    string
		runMode int
	}{{"new", 0}, {"full_slot", runInline}}
	sizes := []struct{ cnum, onum int }{
		{256, 256}, {512, 512}, {1024, 1024}, {1536, 1536}, {2044, 2044}, {2044, 256}, {256, 2044},
	}
	for _, sz := range sizes {
		for _, overlapPct := range []int{0, 50, 100} {
			cv, ov := orArrayBenchValues(sz.cnum, sz.onum, overlapPct)
			src := arrayContainer((int(startIdx)+sz.cnum+3)/4*4, cv)
			other := arrayContainer(int(startIdx)+sz.onum, ov)
			c := make([]uint16, len(src))
			buf := make([]uint16, maxContainerSize)

			for _, mode := range modes {
				for _, route := range routes {
					name := "c" + strconv.Itoa(sz.cnum) + "_o" + strconv.Itoa(sz.onum) + "_overlap" +
						strconv.Itoa(overlapPct) + "/" + mode.name + "/" + route.name
					b.Run(name, func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							copy(c, src)
							route.or(c, other, buf, mode.runMode)
						}
					})
				}
			}
		}
	}
}

// BenchmarkOrArrayShapes times array ∪ array through containerOrAlt on value
// shapes where unionMayFitArray's bound decides the route and where it does not.
//
// go test -v -bench BenchmarkOrArrayShapes -benchmem -run ^$ github.com/weaviate/sroar
func BenchmarkOrArrayShapes(b *testing.B) {
	shapes := []struct {
		name   string
		values func(cnum, onum int) (cv, ov []uint16)
	}{
		{"interleaved_disjoint", func(cnum, onum int) ([]uint16, []uint16) {
			return orArrayBenchValues(cnum, onum, 0)
		}},
		{"interleaved_overlap50", func(cnum, onum int) ([]uint16, []uint16) {
			return orArrayBenchValues(cnum, onum, 50)
		}},
		{"other_out_high", func(cnum, onum int) ([]uint16, []uint16) {
			return uint16Range(0, 2*cnum, 2), uint16Range(0, 4*onum, 4)
		}},
		{"out_both_ends", func(cnum, onum int) ([]uint16, []uint16) {
			from := (4*onum - 2*cnum) / 4 * 2
			from = max(from, 0)
			return uint16Range(from, from+2*cnum, 2), uint16Range(0, 4*onum, 4)
		}},
		{"c_low_other_high", func(cnum, onum int) ([]uint16, []uint16) {
			return uint16Range(0, 2*cnum, 2), uint16Range(cnum/2*2, cnum/2*2+2*onum, 2)
		}},
		{"ranges_disjoint", func(cnum, onum int) ([]uint16, []uint16) {
			return uint16Range(0, 2*cnum, 2), uint16Range(2*cnum, 2*cnum+2*onum, 2)
		}},
		{"c_run", func(cnum, onum int) ([]uint16, []uint16) {
			return uint16Range(0, cnum, 1), uint16Range(cnum/2, cnum/2+2*onum, 2)
		}},
	}
	sizes := []struct{ cnum, onum int }{{1024, 1024}, {1536, 1536}, {2044, 2044}, {2044, 256}}
	for _, sz := range sizes {
		for _, shape := range shapes {
			cv, ov := shape.values(sz.cnum, sz.onum)
			other := arrayContainer(int(startIdx)+len(ov), ov)
			buf := make([]uint16, maxContainerSize)
			for _, mode := range []struct {
				name    string
				runMode int
			}{{"new", 0}, {"full_slot", runInline}} {
				src := arrayContainer((int(startIdx)+len(cv)+3)/4*4, cv)
				c := make([]uint16, len(src))
				name := "c" + strconv.Itoa(sz.cnum) + "_o" + strconv.Itoa(sz.onum) + "/" + shape.name + "/" + mode.name
				b.Run(name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						copy(c, src)
						containerOrAlt(c, other, buf, mode.runMode)
					}
				})
			}
		}
	}
}

// BenchmarkBitsIntoArray compares bitsIntoArray, which reads uint64s, against
// the uint16 loop, on values packed into one run and spread over the container.
//
// go test -v -bench BenchmarkBitsIntoArray -benchmem -run ^$ github.com/weaviate/sroar
func BenchmarkBitsIntoArray(b *testing.B) {
	impls := []struct {
		name string
		fn   func(words, out []uint16, baseWord uint16) int
	}{
		{"uint16", bitsIntoArray16},
		{"uint64", bitsIntoArray},
	}
	for _, n := range []int{64, 512, 2000, 8000, 30000} {
		shapes := []struct {
			name string
			vals []uint16
		}{
			{"clustered", uint16Range(4096, 4096+n, 1)},
			{"spread", uint16Range(0, maxCardinality, maxCardinality/n)[:n]},
		}
		for _, shape := range shapes {
			words := bitmapWords(shape.vals)
			out := make([]uint16, n)
			for _, impl := range impls {
				b.Run(shape.name+"_"+strconv.Itoa(n)+"/"+impl.name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						impl.fn(words, out, 0)
					}
				})
			}
		}
	}
}

// BenchmarkOrValues compares setting each value against ORing a mask per
// uint64 word, on values from clustered to spread over the whole container.
// Steps 21 and 24 put about 3 values in a word, either side of orMaskMinValuesPerWord.
//
// go test -v -bench BenchmarkOrValues -benchmem -run ^$ github.com/weaviate/sroar
func BenchmarkOrValues(b *testing.B) {
	impls := []struct {
		name string
		fn   func(words, vals []uint16) int
	}{
		{"each", orValuesEach},
		{"mask", orValuesByMask},
		{"chosen", orValues},
	}
	for _, c := range []int{256, 1500} {
		spreadStep := maxCardinality / c
		for _, step := range []int{1, 8, 16, 21, 24, 32, 48, spreadStep} {
			if step > spreadStep {
				continue
			}
			shape := "step" + strconv.Itoa(step)
			switch step {
			case 1:
				shape = "clustered"
			case spreadStep:
				shape = "spread"
			}
			vals := uint16Range(0, c*step, step)
			for _, n := range []int{64, 512, 2000, 30000} {
				src := bitmapWords(uint16Range(1, maxCardinality, maxCardinality/n)[:n])
				words := make([]uint16, len(src))
				for _, impl := range impls {
					name := "c" + strconv.Itoa(c) + "_" + shape + "_into" + strconv.Itoa(n) + "/" + impl.name
					b.Run(name, func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							copy(words, src)
							impl.fn(words, vals)
						}
					})
				}
			}
		}
	}
}

// BenchmarkOrArrayBitmap times array ∪ bitmap through containerOrAlt with c in
// a slot that fits the union, in a bitmap-sized slot and not inline.
//
// go test -v -bench BenchmarkOrArrayBitmap -benchmem -run ^$ github.com/weaviate/sroar
func BenchmarkOrArrayBitmap(b *testing.B) {
	buf := make([]uint16, maxContainerSize)
	for _, onum := range []int{64, 512, 2000, 30000} {
		for _, cnum := range []int{256, 1500} {
			cv, ov := orArrayBenchValues(cnum, onum, 50)
			other := arrayContainer(int(startIdx)+onum, ov).toBitmapContainer(nil)
			u := len(sortedUnion(cv, ov))
			modes := []struct {
				name    string
				slot    int
				runMode int
			}{
				{"fit_slot", (int(startIdx) + u + 3) / 4 * 4, runInline},
				{"bitmap_slot", maxContainerSize, runInline},
				{"new", (int(startIdx) + cnum + 3) / 4 * 4, 0},
			}
			for _, mode := range modes {
				if mode.name == "fit_slot" && !isArrayCard(u) {
					continue
				}
				src := arrayContainer(mode.slot, cv)
				c := make([]uint16, len(src))
				name := "o" + strconv.Itoa(onum) + "_c" + strconv.Itoa(cnum) + "/" + mode.name
				b.Run(name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						copy(c, src)
						containerOrAlt(c, other, buf, mode.runMode)
					}
				})
			}
		}
	}
}
