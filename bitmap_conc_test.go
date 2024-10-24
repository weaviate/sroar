package sroar

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

var superset *Bitmap
var subsets []*Bitmap

func init() {
	// randSeed := int64(1724861525311)
	randSeed := time.Now().UnixNano()
	countSubsets := 10
	countElements := 56

	// max element is 3x bigger than capacity of single bm's container
	maxX := (int(math.MaxUint16) + 1) * 3
	// buffers := makeContainerBuffers(countBuffers)
	rnd := rand.New(rand.NewSource(randSeed))

	superset = NewBitmap()
	subsets = make([]*Bitmap, countSubsets)

	for i := 0; i < countElements; i++ {
		x := uint64(rnd.Intn(maxX))
		superset.Set(x)
	}

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
}

// go test -v -bench Benchmark_And_Orig -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_And_Orig(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bm := superset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			bm.And(subsets[j])
		}
	}
}

// go test -v -bench Benchmark_And_Alt -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
func Benchmark_And_Alt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bm := superset.Clone()
		for j, l := 0, len(subsets); j < l; j++ {
			bm.AndAlt(subsets[j])
		}
	}
}

// var bm *Bitmap
// var control []uint64
// var controls [][]uint64

// func init() {
// 	size := 100_000_000
// 	setProb := float32(0.02)
// 	controlProb := float32(0.01)
// 	controlsCount := 1000

// 	bm = NewBitmap()
// 	control = make([]uint64, 0, int(controlProb*float32(size)))
// 	controls = make([][]uint64, controlsCount)

// 	for x := 0; x < size; x++ {
// 		if rand.Float32() < setProb {
// 			bm.Set(uint64(x))
// 		}
// 		if rand.Float32() < controlProb {
// 			control = append(control, uint64(x))
// 		}
// 	}

// 	delta := len(control) / controlsCount
// 	rem := len(control) - delta*controlsCount

// 	from := 0
// 	for i := 0; i < rem; i++ {
// 		to := from + delta + 1
// 		fmt.Printf("f[%d]t[%d] ", from, to)
// 		controls[i] = control[from:to]
// 		from = to
// 	}
// 	for i := rem; i < controlsCount-1; i++ {
// 		to := from + delta
// 		fmt.Printf("f[%d]t[%d] ", from, to)
// 		controls[i] = control[from:to]
// 		from = to
// 	}
// 	controls[controlsCount-1] = control[from:]

// 	fmt.Printf(" ==> num keys [%d]\n", bm.keys.numKeys())
// 	fmt.Printf(" ==> card [%d]\n", bm.GetCardinality())
// 	fmt.Printf(" ==> control [%d]\n", len(control))
// 	// fmt.Printf(" ==> controlsCount [%d]:", controlsCount)
// 	// for i := range controls {
// 	// 	fmt.Printf(" %d", len(controls[i]))
// 	// }
// 	// fmt.Println()
// 	fmt.Println()
// }

// // go test -v -bench Benchmark_Contains -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
// func Benchmark_Contains(b *testing.B) {
// 	for n := 0; n < b.N; n++ {
// 		d := time.Duration(0)
// 		for i := range controls {
// 			for _, x := range controls[i] {
// 				t := time.Now()
// 				bm.Contains(x)
// 				d += time.Since(t)
// 			}
// 		}
// 		fmt.Printf(" ==> duration [%s]\n", d)
// 	}
// }

// // go test -v -bench Benchmark_Contained -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
// func Benchmark_Contained(b *testing.B) {
// 	for n := 0; n < b.N; n++ {
// 		d := time.Duration(0)
// 		for i := range controls {
// 			_, d1 := bm.Contained(controls[i])
// 			d += d1
// 		}
// 		fmt.Printf(" ==> duration [%s]\n", d)
// 	}
// }

// // go test -v -bench Benchmark_Contained2 -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
// func Benchmark_Contained2(b *testing.B) {
// 	bufs := [][]uint16{
// 		make([]uint16, maxContainerSize),
// 		make([]uint16, maxContainerSize),
// 		make([]uint16, maxContainerSize),
// 		make([]uint16, maxContainerSize),
// 	}
// 	for n := 0; n < b.N; n++ {
// 		d := time.Duration(0)
// 		for i := range controls {
// 			_, d1 := bm.Contained2(controls[i], bufs)
// 			d += d1
// 		}
// 		fmt.Printf(" ==> duration [%s]\n", d)
// 	}
// }

// // go test -v -bench Benchmark_ContainedBoth -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof
// func Benchmark_ContainedBoth(b *testing.B) {
// 	bufs := [][]uint16{
// 		make([]uint16, maxContainerSize),
// 		make([]uint16, maxContainerSize),
// 		make([]uint16, maxContainerSize),
// 		make([]uint16, maxContainerSize),
// 	}
// 	for n := 0; n < b.N; n++ {
// 		d1 := time.Duration(0)
// 		d2 := time.Duration(0)
// 		for i := range controls {
// 			// bm.Contained(controls[i])
// 			_, d11 := bm.Contained(controls[i])
// 			d1 += d11

// 			// bm.Contained2(controls[i], bufs)
// 			_, d22 := bm.Contained2(controls[i], bufs)
// 			d2 += d22
// 		}
// 		fmt.Printf(" ==> duration and      [%s]\n", d1)
// 		fmt.Printf(" ==> duration and conc [%s]\n", d2)
// 	}
// }
