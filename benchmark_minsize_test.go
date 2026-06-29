package sroar

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// ============================================================================
//
// Benchmarks measuring the impact of minContainerSize (16 vs 64) on merges.
//
// Methodology: minContainerSize is a compile-time const, so these are run
// twice — once on the current value, once with container.go flipped to the
// other value — and compared with benchstat. Example:
//
//   go test -run ^$ -bench 'BenchmarkMin' -benchmem -count=10 > a.txt
//   # edit container.go: minContainerSize = 64
//   go test -run ^$ -bench 'BenchmarkMin' -benchmem -count=10 > b.txt
//   git checkout container.go
//   benchstat b.txt a.txt
//
// In addition to the usual ns/op, B/op, allocs/op, every benchmark reports
// a deterministic "memMoved/op" metric (uint16 words moved by scoot/expand),
// which is the noise-free structural signal for how much data shuffling the
// chosen minContainerSize caused.
//
// Density = elements per container (each container spans a 64K key range).
// The density buckets straddle the array size ladder so the effect of the two
// extra rungs that minContainerSize=16 adds (16->32->64) is visible:
//   8    -> fits min array (<=16 with min=16; <=64 with min=64)
//   28   -> needs size 32 (min=16) vs fits 64 (min=64)
//   60   -> size 64 either way
//   250  -> size 256
//   2000 -> size 2048
//   6000 -> dense -> bitmap container
//
// ============================================================================

const minsizeSeed = int64(1724861525311)

// containerVal returns a value that lands in the container identified by
// containerKey (its low 16 bits = low).
func containerVal(containerKey int, low uint16) uint64 {
	return uint64(containerKey)<<16 | uint64(low)
}

// buildBitmap creates a bitmap with numContainers containers (keys 0..n-1),
// each holding density distinct-ish elements drawn from rnd.
func buildBitmap(numContainers, density int, rnd *rand.Rand) *Bitmap {
	bm := NewBitmap()
	for k := 0; k < numContainers; k++ {
		for j := 0; j < density; j++ {
			bm.Set(containerVal(k, uint16(rnd.Intn(maxCardinality))))
		}
	}
	return bm
}

var minsizeConfigs = []struct {
	name          string
	numContainers int
	density       int
}{
	{"K1024/dens8", 1024, 8},
	{"K1024/dens28", 1024, 28},
	{"K1024/dens60", 1024, 60},
	{"K1024/dens250", 1024, 250},
	{"K1024/dens2000", 1024, 2000},
	{"K1024/dens6000", 1024, 6000},
	{"K64/dens60", 64, 60},
	{"K64/dens6000", 64, 6000},
}

// BenchmarkMinOrLarge measures the dominant Or path: sequential in-place
// merges into a base bitmap with >10 containers. Containers grow via
// append-and-forget (no tail-shift), so this is where we expect min=16 to be
// neutral-to-better (smaller appended containers, smaller orphans).
func BenchmarkMinOrLarge(b *testing.B) {
	const numDeltas = 8
	for _, cfg := range minsizeConfigs {
		b.Run(cfg.name, func(b *testing.B) {
			rnd := rand.New(rand.NewSource(minsizeSeed))
			perDelta := cfg.density / numDeltas
			if perDelta == 0 {
				perDelta = 1
			}
			base := buildBitmap(cfg.numContainers, perDelta, rnd)
			baseBuf := base.ToBuffer()
			deltas := make([]*Bitmap, numDeltas)
			for d := range deltas {
				deltas[d] = buildBitmap(cfg.numContainers, perDelta, rnd)
			}

			b.ResetTimer()
			var memMoved int64
			for i := 0; i < b.N; i++ {
				res := FromBufferWithCopy(baseBuf)
				for _, delta := range deltas {
					res.Or(delta)
				}
				memMoved += int64(res.memMoved)
			}
			b.ReportMetric(float64(memMoved)/float64(b.N), "memMoved/op")
		})
	}
}

// BenchmarkMinOrSmall exercises the an<=10 branch, the only Or path that
// tail-shifts in place (insertAt -> scootRight). Kept small on purpose.
func BenchmarkMinOrSmall(b *testing.B) {
	const numDeltas = 8
	densities := []int{8, 28, 60, 250, 2000}
	for _, density := range densities {
		b.Run(fmt.Sprintf("K8/dens%d", density), func(b *testing.B) {
			rnd := rand.New(rand.NewSource(minsizeSeed))
			perDelta := density / numDeltas
			if perDelta == 0 {
				perDelta = 1
			}
			base := buildBitmap(8, perDelta, rnd)
			baseBuf := base.ToBuffer()
			deltas := make([]*Bitmap, numDeltas)
			for d := range deltas {
				deltas[d] = buildBitmap(8, perDelta, rnd)
			}

			b.ResetTimer()
			var memMoved int64
			for i := 0; i < b.N; i++ {
				res := FromBufferWithCopy(baseBuf)
				for _, delta := range deltas {
					res.Or(delta)
				}
				memMoved += int64(res.memMoved)
			}
			b.ReportMetric(float64(memMoved)/float64(b.N), "memMoved/op")
		})
	}
}

// BenchmarkMinAnd confirms And is neutral: it only shrinks containers
// (scootLeft), never crosses the size ladder upward.
func BenchmarkMinAnd(b *testing.B) {
	for _, cfg := range minsizeConfigs {
		b.Run(cfg.name, func(b *testing.B) {
			rnd := rand.New(rand.NewSource(minsizeSeed))
			base := buildBitmap(cfg.numContainers, cfg.density, rnd)
			other := buildBitmap(cfg.numContainers, cfg.density/2+1, rnd)
			baseBuf := base.ToBuffer()

			b.ResetTimer()
			var memMoved int64
			for i := 0; i < b.N; i++ {
				res := FromBufferWithCopy(baseBuf)
				res.And(other)
				memMoved += int64(res.memMoved)
			}
			b.ReportMetric(float64(memMoved)/float64(b.N), "memMoved/op")
		})
	}
}

// BenchmarkMinSetIncremental is the genuinely min-sensitive surface: building
// a bitmap element-by-element. Interleaved Sets across containers grow each
// container gradually, so min=16 climbs two extra rungs of the doubling ladder
// (16->32->64), each crossing triggering a scootRight tail-shift. This is the
// real cost to weigh against the per-container disk-size saving.
func BenchmarkMinSetIncremental(b *testing.B) {
	configs := []struct {
		name          string
		numContainers int
		density       int
	}{
		{"K1024/dens8", 1024, 8},
		{"K1024/dens28", 1024, 28},
		{"K1024/dens60", 1024, 60},
		{"K1024/dens250", 1024, 250},
		{"K1024/dens2000", 1024, 2000},
	}
	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			rnd := rand.New(rand.NewSource(minsizeSeed))
			// Pre-generate an interleaved insertion order (round-robin across
			// containers) so containers grow concurrently and gradually.
			vals := make([]uint64, 0, cfg.numContainers*cfg.density)
			for j := 0; j < cfg.density; j++ {
				for k := 0; k < cfg.numContainers; k++ {
					vals = append(vals, containerVal(k, uint16(rnd.Intn(maxCardinality))))
				}
			}

			b.ResetTimer()
			var memMoved int64
			for i := 0; i < b.N; i++ {
				bm := NewBitmap()
				for _, v := range vals {
					bm.Set(v)
				}
				memMoved += int64(bm.memMoved)
			}
			b.ReportMetric(float64(memMoved)/float64(b.N), "memMoved/op")
		})
	}
}

// BenchmarkMinSetSequential builds the same bitmaps but inserts values in
// globally ascending order — the realistic ingestion pattern (e.g. increasing
// doc IDs). This is the access pattern that in-order fast paths (append-without-
// search/shift, last-container reuse) are designed to exploit; the round-robin
// BenchmarkMinSetIncremental is the adversarial worst case for them.
func BenchmarkMinSetSequential(b *testing.B) {
	configs := []struct {
		name          string
		numContainers int
		density       int
	}{
		{"K1024/dens8", 1024, 8},
		{"K1024/dens28", 1024, 28},
		{"K1024/dens60", 1024, 60},
		{"K1024/dens250", 1024, 250},
		{"K1024/dens2000", 1024, 2000},
	}
	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			rnd := rand.New(rand.NewSource(minsizeSeed))
			// Build globally ascending values: container by container, and within
			// each container distinct lows in ascending order.
			vals := make([]uint64, 0, cfg.numContainers*cfg.density)
			for k := 0; k < cfg.numContainers; k++ {
				seen := make(map[uint16]struct{}, cfg.density)
				lows := make([]uint16, 0, cfg.density)
				for len(lows) < cfg.density {
					l := uint16(rnd.Intn(maxCardinality))
					if _, ok := seen[l]; ok {
						continue
					}
					seen[l] = struct{}{}
					lows = append(lows, l)
				}
				slices.Sort(lows)
				for _, l := range lows {
					vals = append(vals, containerVal(k, l))
				}
			}

			b.ResetTimer()
			var memMoved int64
			for i := 0; i < b.N; i++ {
				bm := NewBitmap()
				for _, v := range vals {
					bm.Set(v)
				}
				memMoved += int64(bm.memMoved)
			}
			b.ReportMetric(float64(memMoved)/float64(b.N), "memMoved/op")
		})
	}
}

// BenchmarkRemoveSequential removes every element in globally-ascending order
// (container by container, ascending lows within each) — the clustered pattern
// the last-container cache in Remove is designed to exploit, mirroring
// BenchmarkMinSetSequential on the Set side. The build is excluded from timing;
// only the Remove loop is measured.
func BenchmarkRemoveSequential(b *testing.B) {
	configs := []struct {
		name          string
		numContainers int
		density       int
	}{
		{"K1024/dens8", 1024, 8},
		{"K1024/dens28", 1024, 28},
		{"K1024/dens60", 1024, 60},
		{"K1024/dens250", 1024, 250},
		{"K1024/dens2000", 1024, 2000},
	}
	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			rnd := rand.New(rand.NewSource(minsizeSeed))
			// Build globally ascending values: container by container, and within
			// each container distinct lows in ascending order.
			vals := make([]uint64, 0, cfg.numContainers*cfg.density)
			for k := 0; k < cfg.numContainers; k++ {
				seen := make(map[uint16]struct{}, cfg.density)
				lows := make([]uint16, 0, cfg.density)
				for len(lows) < cfg.density {
					l := uint16(rnd.Intn(maxCardinality))
					if _, ok := seen[l]; ok {
						continue
					}
					seen[l] = struct{}{}
					lows = append(lows, l)
				}
				slices.Sort(lows)
				for _, l := range lows {
					vals = append(vals, containerVal(k, l))
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				bm := NewBitmap()
				for _, v := range vals {
					bm.Set(v)
				}
				b.StartTimer()
				for _, v := range vals {
					bm.Remove(v)
				}
			}
		})
	}
}
