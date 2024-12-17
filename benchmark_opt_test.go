package sroar

import (
	// "math/rand"
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

// func init() {
// 	randSeed := int64(1724861525311)
// 	// randSeed := time.Now().UnixNano()
// 	countSubsets := 10
// 	countElements := 1234567

// 	// max element is 3x bigger than capacity of single bm's container
// 	maxX := maxCardinality * 3
// 	rnd := rand.New(rand.NewSource(randSeed))

// 	superset = NewBitmap()
// 	for i := 0; i < countElements; i++ {
// 		x := uint64(rnd.Intn(maxX))
// 		superset.Set(x)
// 	}
// 	bigset = superset.Clone()

// 	subsets = make([]*Bitmap, countSubsets)
// 	for i := range subsets {
// 		subsets[i] = NewBitmap()
// 		// each next subset bitmap contains fewer elements
// 		// 1/2 of countElements, 1/3, 1/4, ...
// 		for j, c := 0, countElements/(i+2); j < c; j++ {
// 			x := uint64(rnd.Intn(maxX))
// 			subsets[i].Set(x)
// 			// ensure superset contains element of subset
// 			superset.Set(x)
// 		}
// 	}
// }

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

/*

AND
go test -v -bench Benchmark_And_ -benchmem -run ^$ github.com/weaviate/sroar


elements: 7

Benchmark_And_Old
Benchmark_And_Old-10              354009              3370 ns/op            7488 B/op         84 allocs/op
Benchmark_And_OldFn
Benchmark_And_OldFn-10            222190              5025 ns/op           15488 B/op        175 allocs/op
Benchmark_And_Alt
Benchmark_And_Alt-10              952080              1258 ns/op            3008 B/op         14 allocs/op
Benchmark_And_AltFn
Benchmark_And_AltFn-10            243397              5046 ns/op           16224 B/op        136 allocs/op
Benchmark_And_Buf
Benchmark_And_Buf-10              767902              1396 ns/op            2240 B/op          8 allocs/op
Benchmark_And_BufFn
Benchmark_And_BufFn-10            243042              4728 ns/op           15456 B/op        130 allocs/op




elements: 67

Benchmark_And_Old
Benchmark_And_Old-10              201232              5170 ns/op           10040 B/op        132 allocs/op
Benchmark_And_OldFn
Benchmark_And_OldFn-10            203049              5736 ns/op           16696 B/op        182 allocs/op
Benchmark_And_Alt
Benchmark_And_Alt-10              422576              2542 ns/op            5120 B/op         26 allocs/op
Benchmark_And_AltFn
Benchmark_And_AltFn-10            202192              5640 ns/op           19392 B/op        150 allocs/op
Benchmark_And_Buf
Benchmark_And_Buf-10              463312              2259 ns/op            2816 B/op          8 allocs/op
Benchmark_And_BufFn
Benchmark_And_BufFn-10            208471              5386 ns/op           17088 B/op        132 allocs/op




elements: 567

Benchmark_And_Old-10               97934             10635 ns/op           54752 B/op        132 allocs/op
Benchmark_And_OldFn
Benchmark_And_OldFn-10            112282             10324 ns/op           35936 B/op        185 allocs/op
Benchmark_And_Alt
Benchmark_And_Alt-10              174502              6395 ns/op           20288 B/op         26 allocs/op
Benchmark_And_AltFn
Benchmark_And_AltFn-10            114302              9827 ns/op           36640 B/op        151 allocs/op
Benchmark_And_Buf
Benchmark_And_Buf-10              185132              6142 ns/op           16960 B/op          8 allocs/op
Benchmark_And_BufFn
Benchmark_And_BufFn-10            124104              9480 ns/op           33312 B/op        133 allocs/op




elements: 4567

Benchmark_And_Old
Benchmark_And_Old-10               33253             33912 ns/op          269208 B/op        132 allocs/op
Benchmark_And_OldFn
Benchmark_And_OldFn-10             38859             30867 ns/op          128216 B/op        196 allocs/op
Benchmark_And_Alt
Benchmark_And_Alt-10               33690             35775 ns/op          105792 B/op         35 allocs/op
Benchmark_And_AltFn
Benchmark_And_AltFn-10             38217             31425 ns/op          141280 B/op        167 allocs/op
Benchmark_And_Buf
Benchmark_And_Buf-10               34365             34321 ns/op           81984 B/op          8 allocs/op
Benchmark_And_BufFn
Benchmark_And_BufFn-10             41220             28990 ns/op          119136 B/op        140 allocs/op




elements: 34567

Benchmark_And_Old
Benchmark_And_Old-10                7149            164213 ns/op         1236544 B/op        136 allocs/op
Benchmark_And_OldFn
Benchmark_And_OldFn-10              8340            134304 ns/op          756256 B/op        221 allocs/op
Benchmark_And_Alt
Benchmark_And_Alt-10               17785             67521 ns/op          127808 B/op         26 allocs/op
Benchmark_And_AltFn
Benchmark_And_AltFn-10             12860             92218 ns/op          790848 B/op        196 allocs/op
Benchmark_And_Buf
Benchmark_And_Buf-10               18877             63931 ns/op          109376 B/op          8 allocs/op
Benchmark_And_BufFn
Benchmark_And_BufFn-10             15406             84696 ns/op          533824 B/op        155 allocs/op




elements: 234567

Benchmark_And_Old
Benchmark_And_Old-10                1790            575283 ns/op         4326210 B/op        144 allocs/op
Benchmark_And_OldFn
Benchmark_And_OldFn-10              2811            418000 ns/op         2645825 B/op        300 allocs/op
Benchmark_And_Alt
Benchmark_And_Alt-10               15022             79385 ns/op          109376 B/op          8 allocs/op
Benchmark_And_AltFn
Benchmark_And_AltFn-10              4418            270359 ns/op         2645696 B/op        292 allocs/op
Benchmark_And_Buf
Benchmark_And_Buf-10               14794             80532 ns/op          109376 B/op          8 allocs/op
Benchmark_And_BufFn
Benchmark_And_BufFn-10              5156            214184 ns/op         1812160 B/op        204 allocs/op




elements: 1234567

Benchmark_And_Old
Benchmark_And_Old-10                1976            566599 ns/op         4326210 B/op        144 allocs/op
Benchmark_And_OldFn
Benchmark_And_OldFn-10              2055            596698 ns/op         4012738 B/op        368 allocs/op
Benchmark_And_Alt
Benchmark_And_Alt-10               10000            106026 ns/op          109376 B/op          8 allocs/op
Benchmark_And_AltFn
Benchmark_And_AltFn-10              3192            410258 ns/op         4012738 B/op        368 allocs/op
Benchmark_And_Buf
Benchmark_And_Buf-10               10000            107473 ns/op          109376 B/op          8 allocs/op
Benchmark_And_BufFn
Benchmark_And_BufFn-10              3608            344070 ns/op         2876096 B/op        248 allocs/op




AND_NOT
go test -v -bench Benchmark_AndNot_ -benchmem -run ^$ github.com/weaviate/sroar


elements: 7

Benchmark_AndNot_Old
Benchmark_AndNot_Old-10            70926             15781 ns/op          387736 B/op        196 allocs/op
Benchmark_AndNot_Alt
Benchmark_AndNot_Alt-10           504362              2139 ns/op            6336 B/op         40 allocs/op
Benchmark_AndNot_AltFn
Benchmark_AndNot_AltFn-10          95991             12127 ns/op           54080 B/op        240 allocs/op
Benchmark_AndNot_Buf
Benchmark_AndNot_Buf-10           626000              1694 ns/op            2240 B/op          8 allocs/op
Benchmark_AndNot_BufFn
Benchmark_AndNot_BufFn-10          99133             11999 ns/op           49984 B/op        208 allocs/op




elements: 67

Benchmark_AndNot_Old
Benchmark_AndNot_Old-10            44391             26830 ns/op          430320 B/op        300 allocs/op
Benchmark_AndNot_Alt
Benchmark_AndNot_Alt-10           144529              8214 ns/op           19584 B/op        128 allocs/op
Benchmark_AndNot_AltFn
Benchmark_AndNot_AltFn-10          57662             20955 ns/op           76544 B/op        329 allocs/op
Benchmark_AndNot_Buf
Benchmark_AndNot_Buf-10           193064              5868 ns/op            2816 B/op          8 allocs/op
Benchmark_AndNot_BufFn
Benchmark_AndNot_BufFn-10          66603             17891 ns/op           59776 B/op        209 allocs/op




elements: 567

Benchmark_AndNot_Old
Benchmark_AndNot_Old-10            11874             99060 ns/op          723104 B/op        301 allocs/op
Benchmark_AndNot_Alt
Benchmark_AndNot_Alt-10            22927             51917 ns/op          119360 B/op        128 allocs/op
Benchmark_AndNot_AltFn
Benchmark_AndNot_AltFn-10          14223             92467 ns/op          417024 B/op        368 allocs/op
Benchmark_AndNot_Buf
Benchmark_AndNot_Buf-10            26943             44218 ns/op           16960 B/op          8 allocs/op
Benchmark_AndNot_BufFn
Benchmark_AndNot_BufFn-10          16243             73520 ns/op          314624 B/op        248 allocs/op




elements: 4567

Benchmark_AndNot_Old
Benchmark_AndNot_Old-10             3001            399416 ns/op         3017282 B/op        184 allocs/op
Benchmark_AndNot_Alt
Benchmark_AndNot_Alt-10             4743            229674 ns/op          327744 B/op         68 allocs/op
Benchmark_AndNot_AltFn
Benchmark_AndNot_AltFn-10           2870            398317 ns/op         2940865 B/op        368 allocs/op
Benchmark_AndNot_Buf
Benchmark_AndNot_Buf-10             5602            207516 ns/op           81984 B/op          8 allocs/op
Benchmark_AndNot_BufFn
Benchmark_AndNot_BufFn-10           3097            361907 ns/op         2126785 B/op        248 allocs/op




elements: 34567

Benchmark_AndNot_Old-10             1551            700261 ns/op         3568449 B/op         64 allocs/op
Benchmark_AndNot_Alt
Benchmark_AndNot_Alt-10             4080            283370 ns/op          109376 B/op          8 allocs/op
Benchmark_AndNot_AltFn
Benchmark_AndNot_AltFn-10           2102            547419 ns/op         4012738 B/op        368 allocs/op
Benchmark_AndNot_Buf
Benchmark_AndNot_Buf-10             4062            293039 ns/op          109376 B/op          8 allocs/op
Benchmark_AndNot_BufFn
Benchmark_AndNot_BufFn-10           2358            482354 ns/op         2876097 B/op        248 allocs/op




elements: 234567

Benchmark_AndNot_Old
Benchmark_AndNot_Old-10             2131            498089 ns/op         3568448 B/op         64 allocs/op
Benchmark_AndNot_Alt
Benchmark_AndNot_Alt-10            10000            106267 ns/op          109376 B/op          8 allocs/op
Benchmark_AndNot_AltFn
Benchmark_AndNot_AltFn-10           3237            418951 ns/op         4012736 B/op        368 allocs/op
Benchmark_AndNot_Buf
Benchmark_AndNot_Buf-10            10000            108025 ns/op          109376 B/op          8 allocs/op
Benchmark_AndNot_BufFn
Benchmark_AndNot_BufFn-10           3405            337600 ns/op         2876098 B/op        248 allocs/op




elements: 1234567

Benchmark_AndNot_Old
Benchmark_AndNot_Old-10             2197            496162 ns/op         3568451 B/op         64 allocs/op
Benchmark_AndNot_Alt
Benchmark_AndNot_Alt-10            12770             93715 ns/op          109376 B/op          8 allocs/op
Benchmark_AndNot_AltFn
Benchmark_AndNot_AltFn-10           3794            336233 ns/op         3247296 B/op        328 allocs/op
Benchmark_AndNot_Buf
Benchmark_AndNot_Buf-10            12559             95420 ns/op          109376 B/op          8 allocs/op
Benchmark_AndNot_BufFn
Benchmark_AndNot_BufFn-10           4340            278626 ns/op         2243265 B/op        222 allocs/op




OR
go test -v -bench Benchmark_Or_ -benchmem -run ^$ github.com/weaviate/sroar


elements: 7

Benchmark_Or_Old
Benchmark_Or_Old-10               131812              8803 ns/op            2240 B/op          8 allocs/op
Benchmark_Or_OldFn
Benchmark_Or_OldFn-10              62971             18365 ns/op           35520 B/op        178 allocs/op
Benchmark_Or_Alt
Benchmark_Or_Alt-10               192248              5902 ns/op            2240 B/op          8 allocs/op
Benchmark_Or_AltFn
Benchmark_Or_AltFn-10              70018             17027 ns/op           49984 B/op        208 allocs/op
Benchmark_Or_Buf
Benchmark_Or_Buf-10               610615              1733 ns/op            2240 B/op          8 allocs/op
Benchmark_Or_BufFn
Benchmark_Or_BufFn-10              97219             12173 ns/op           49984 B/op        208 allocs/op




elements: 67

Benchmark_Or_Old
Benchmark_Or_Old-10                64905             15918 ns/op            7936 B/op         12 allocs/op
Benchmark_Or_OldFn
Benchmark_Or_OldFn-10              46651             25897 ns/op           60032 B/op        210 allocs/op
Benchmark_Or_Alt
Benchmark_Or_Alt-10                80311             14288 ns/op            4864 B/op         10 allocs/op
Benchmark_Or_AltFn
Benchmark_Or_AltFn-10              45244             26713 ns/op           66112 B/op        209 allocs/op
Benchmark_Or_Buf
Benchmark_Or_Buf-10               151845              7379 ns/op            4864 B/op         10 allocs/op
Benchmark_Or_BufFn
Benchmark_Or_BufFn-10              58843             19765 ns/op           66112 B/op        209 allocs/op




elements: 567

Benchmark_Or_Old
Benchmark_Or_Old-10                15976             73044 ns/op           36928 B/op         12 allocs/op
Benchmark_Or_OldFn
Benchmark_Or_OldFn-10               9153            111088 ns/op          388032 B/op        248 allocs/op
Benchmark_Or_Alt
Benchmark_Or_Alt-10                16340             72596 ns/op           36928 B/op         12 allocs/op
Benchmark_Or_AltFn
Benchmark_Or_AltFn-10              10122            118162 ns/op          566656 B/op        248 allocs/op
Benchmark_Or_Buf
Benchmark_Or_Buf-10                18055             64807 ns/op           36928 B/op         12 allocs/op
Benchmark_Or_BufFn
Benchmark_Or_BufFn-10               9344            110268 ns/op          566656 B/op        248 allocs/op




elements: 4567

Benchmark_Or_Old
Benchmark_Or_Old-10                10137            115462 ns/op          136512 B/op         10 allocs/op
Benchmark_Or_OldFn
Benchmark_Or_OldFn-10               2520            434470 ns/op         2429763 B/op        248 allocs/op
Benchmark_Or_Alt
Benchmark_Or_Alt-10                 9092            121514 ns/op          136512 B/op         10 allocs/op
Benchmark_Or_AltFn
Benchmark_Or_AltFn-10               3600            310866 ns/op         2740417 B/op        248 allocs/op
Benchmark_Or_Buf
Benchmark_Or_Buf-10                 9693            115285 ns/op          136512 B/op         10 allocs/op
Benchmark_Or_BufFn
Benchmark_Or_BufFn-10               3861            304141 ns/op         2740417 B/op        248 allocs/op




elements: 34567

Benchmark_Or_Old
Benchmark_Or_Old-10                 4082            282748 ns/op          109377 B/op          8 allocs/op
Benchmark_Or_OldFn
Benchmark_Or_OldFn-10               2444            468277 ns/op         2876099 B/op        248 allocs/op
Benchmark_Or_Alt
Benchmark_Or_Alt-10                 4143            285164 ns/op          109376 B/op          8 allocs/op
Benchmark_Or_AltFn
Benchmark_Or_AltFn-10               2473            476289 ns/op         2876097 B/op        248 allocs/op
Benchmark_Or_Buf
Benchmark_Or_Buf-10                 4249            270790 ns/op          109376 B/op          8 allocs/op
Benchmark_Or_BufFn
Benchmark_Or_BufFn-10               2497            469248 ns/op         2876099 B/op        248 allocs/op




elements: 234567

Benchmark_Or_Old
Benchmark_Or_Old-10                 3280            329067 ns/op          109377 B/op          8 allocs/op
Benchmark_Or_OldFn
Benchmark_Or_OldFn-10               2298            511944 ns/op         2876099 B/op        248 allocs/op
Benchmark_Or_Alt
Benchmark_Or_Alt-10                10000            113039 ns/op          109376 B/op          8 allocs/op
Benchmark_Or_AltFn
Benchmark_Or_AltFn-10               3778            327541 ns/op         2876097 B/op        248 allocs/op
Benchmark_Or_Buf
Benchmark_Or_Buf-10                10000            107026 ns/op          109376 B/op          8 allocs/op
Benchmark_Or_BufFn
Benchmark_Or_BufFn-10               3812            347066 ns/op         2876096 B/op        248 allocs/op




elements: 1234567

cpu: Apple M1 Pro
Benchmark_Or_Old
Benchmark_Or_Old-10                13922             82911 ns/op          109376 B/op          8 allocs/op
Benchmark_Or_OldFn
Benchmark_Or_OldFn-10               4374            310837 ns/op         2876097 B/op        248 allocs/op
Benchmark_Or_Alt
Benchmark_Or_Alt-10                32383             39850 ns/op          109376 B/op          8 allocs/op
Benchmark_Or_AltFn
Benchmark_Or_AltFn-10               4614            268684 ns/op         2876096 B/op        248 allocs/op
Benchmark_Or_Buf
Benchmark_Or_Buf-10                39265             30218 ns/op          109376 B/op          8 allocs/op
Benchmark_Or_BufFn
Benchmark_Or_BufFn-10               4801            255472 ns/op         2876096 B/op        248 allocs/op

*/
