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

// go test -v -bench Benchmark_And_Buf -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_And_Buf(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := make([]uint16, maxContainerSize)
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.AndBuf(subsets[j], buf)
			s2.AndBuf(subsets[l-j-1], buf)
			b1.AndBuf(subsets[j], buf)
			b2.AndBuf(subsets[l-j-1], buf)
		}
	}
}

// go test -v -bench Benchmark_And_BufFn -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_And_BufFn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := make([]uint16, maxContainerSize)
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1 = AndBuf(s1, subsets[j], buf)
			s2 = AndBuf(s2, subsets[l-j-1], buf)
			b1 = AndBuf(b1, subsets[j], buf)
			b2 = AndBuf(b2, subsets[l-j-1], buf)
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

func Benchmark_And_Alt_ConcBuf_1(b *testing.B) {
	benchmark_And_ConcBuf(b, bufs10[:1])
}

func Benchmark_And_Alt_ConcBuf_2(b *testing.B) {
	benchmark_And_ConcBuf(b, bufs10[:2])
}

func Benchmark_And_Alt_ConcBuf_3(b *testing.B) {
	benchmark_And_ConcBuf(b, bufs10[:3])
}

func Benchmark_And_Alt_ConcBuf_4(b *testing.B) {
	benchmark_And_ConcBuf(b, bufs10[:4])
}

func Benchmark_And_Alt_ConcBuf_5(b *testing.B) {
	benchmark_And_ConcBuf(b, bufs10[:5])
}

func Benchmark_And_Alt_ConcBuf_6(b *testing.B) {
	benchmark_And_ConcBuf(b, bufs10[:6])
}

func Benchmark_And_Alt_ConcBuf_7(b *testing.B) {
	benchmark_And_ConcBuf(b, bufs10[:7])
}

func Benchmark_And_Alt_ConcBuf_8(b *testing.B) {
	benchmark_And_ConcBuf(b, bufs10[:8])
}

func Benchmark_And_Alt_ConcBuf_9(b *testing.B) {
	benchmark_And_ConcBuf(b, bufs10[:9])
}

func Benchmark_And_Alt_ConcBuf_10(b *testing.B) {
	benchmark_And_ConcBuf(b, bufs10)
}

func benchmark_And_ConcBuf(b *testing.B, bufs [][]uint16) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.AndConcBuf(subsets[j], bufs...)
			s2.AndConcBuf(subsets[l-j-1], bufs...)
			b1.AndConcBuf(subsets[j], bufs...)
			b2.AndConcBuf(subsets[l-j-1], bufs...)
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

// go test -v -bench Benchmark_AndNot_Buf -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_AndNot_Buf(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := make([]uint16, maxContainerSize)
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.AndNotBuf(subsets[j], buf)
			s2.AndNotBuf(subsets[l-j-1], buf)
			b1.AndNotBuf(subsets[j], buf)
			b2.AndNotBuf(subsets[l-j-1], buf)
		}
	}
}

// go test -v -bench Benchmark_AndNot_BufFn -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_AndNot_BufFn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := make([]uint16, maxContainerSize)
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1 = AndNotBuf(s1, subsets[j], buf)
			s2 = AndNotBuf(s2, subsets[l-j-1], buf)
			b1 = AndNotBuf(b1, subsets[j], buf)
			b2 = AndNotBuf(b2, subsets[l-j-1], buf)
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

func Benchmark_AndNot_Alt_ConcBuf_1(b *testing.B) {
	benchmark_AndNot_ConcBuf(b, bufs10[:1])
}

func Benchmark_AndNot_Alt_ConcBuf_2(b *testing.B) {
	benchmark_AndNot_ConcBuf(b, bufs10[:2])
}

func Benchmark_AndNot_Alt_ConcBuf_3(b *testing.B) {
	benchmark_AndNot_ConcBuf(b, bufs10[:3])
}

func Benchmark_AndNot_Alt_ConcBuf_4(b *testing.B) {
	benchmark_AndNot_ConcBuf(b, bufs10[:4])
}

func Benchmark_AndNot_Alt_ConcBuf_5(b *testing.B) {
	benchmark_AndNot_ConcBuf(b, bufs10[:5])
}

func Benchmark_AndNot_Alt_ConcBuf_6(b *testing.B) {
	benchmark_AndNot_ConcBuf(b, bufs10[:6])
}

func Benchmark_AndNot_Alt_ConcBuf_7(b *testing.B) {
	benchmark_AndNot_ConcBuf(b, bufs10[:7])
}

func Benchmark_AndNot_Alt_ConcBuf_8(b *testing.B) {
	benchmark_AndNot_ConcBuf(b, bufs10[:8])
}

func Benchmark_AndNot_Alt_ConcBuf_9(b *testing.B) {
	benchmark_AndNot_ConcBuf(b, bufs10[:9])
}

func Benchmark_AndNot_Alt_ConcBuf_10(b *testing.B) {
	benchmark_AndNot_ConcBuf(b, bufs10)
}

func benchmark_AndNot_ConcBuf(b *testing.B, bufs [][]uint16) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.AndNotConcBuf(subsets[j], bufs...)
			s2.AndNotConcBuf(subsets[l-j-1], bufs...)
			b1.AndNotConcBuf(subsets[j], bufs...)
			b2.AndNotConcBuf(subsets[l-j-1], bufs...)
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

// go test -v -bench Benchmark_Or_Buf -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_Or_Buf(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := make([]uint16, maxContainerSize)
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.OrBuf(subsets[j], buf)
			s2.OrBuf(subsets[l-j-1], buf)
			b1.OrBuf(subsets[j], buf)
			b2.OrBuf(subsets[l-j-1], buf)
		}
	}
}

// go test -v -bench Benchmark_Or_BufFn -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_Or_BufFn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := make([]uint16, maxContainerSize)
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1 = OrBuf(s1, subsets[j], buf)
			s2 = OrBuf(s2, subsets[l-j-1], buf)
			b1 = OrBuf(b1, subsets[j], buf)
			b2 = OrBuf(b2, subsets[l-j-1], buf)
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

func Benchmark_Or_Alt_ConcBuf_1(b *testing.B) {
	benchmark_Or_ConcBuf(b, bufs10[:1])
}

func Benchmark_Or_Alt_ConcBuf_2(b *testing.B) {
	benchmark_Or_ConcBuf(b, bufs10[:2])
}

func Benchmark_Or_Alt_ConcBuf_3(b *testing.B) {
	benchmark_Or_ConcBuf(b, bufs10[:3])
}

func Benchmark_Or_Alt_ConcBuf_4(b *testing.B) {
	benchmark_Or_ConcBuf(b, bufs10[:4])
}

func Benchmark_Or_Alt_ConcBuf_5(b *testing.B) {
	benchmark_Or_ConcBuf(b, bufs10[:5])
}

func Benchmark_Or_Alt_ConcBuf_6(b *testing.B) {
	benchmark_Or_ConcBuf(b, bufs10[:6])
}

func Benchmark_Or_Alt_ConcBuf_7(b *testing.B) {
	benchmark_Or_ConcBuf(b, bufs10[:7])
}

func Benchmark_Or_Alt_ConcBuf_8(b *testing.B) {
	benchmark_Or_ConcBuf(b, bufs10[:8])
}

func Benchmark_Or_Alt_ConcBuf_9(b *testing.B) {
	benchmark_Or_ConcBuf(b, bufs10[:9])
}

func Benchmark_Or_Alt_ConcBuf_10(b *testing.B) {
	benchmark_Or_ConcBuf(b, bufs10)
}

func benchmark_Or_ConcBuf(b *testing.B, bufs [][]uint16) {
	for i := 0; i < b.N; i++ {
		s1 := superset.Clone()
		s2 := superset.Clone()
		b1 := bigset.Clone()
		b2 := bigset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			s1.OrConcBuf(subsets[j], bufs...)
			s2.OrConcBuf(subsets[l-j-1], bufs...)
			b1.OrConcBuf(subsets[j], bufs...)
			b2.OrConcBuf(subsets[l-j-1], bufs...)
		}
	}
}