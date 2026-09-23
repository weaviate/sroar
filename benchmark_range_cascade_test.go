package sroar

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
)

// Models the merge cascade a bit-sliced numeric range filter drives, at
// production shape. Timing includes GetCardinality so any deferred header
// cost lands in the result. Uses only released APIs, so it also runs as a
// baseline on old checkouts.

// cascadeShape describes the production-shaped operand set: 369 bitmap
// containers per plane at ~24.2M values, matching a single shard's range planes.
type cascadeShape struct {
	containers int
	planes     int
	// densityBits: fill is 1 - 2^-densityBits, high enough to keep containers
	// non-empty/non-full bitmaps so every pass does real work.
	densityBits int
}

var productionCascade = cascadeShape{containers: 369, planes: 15, densityBits: 4}

// buildDensePlanes writes containers directly; Set would need 24M calls per plane.
func buildDensePlanes(shape cascadeShape, seed int64) []*Bitmap {
	rnd := rand.New(rand.NewSource(seed))
	planes := make([]*Bitmap, shape.planes)
	for p := range planes {
		bm := NewBitmapWith(shape.containers + 2)
		for k := 0; k < shape.containers; k++ {
			offset := bm.newContainer(uint16(maxContainerSize))
			c := bm.getContainer(offset)
			c[indexSize] = uint16(maxContainerSize)
			c[indexType] = typeBitmap
			words := uint16To64SliceUnsafe(c[startIdx:])
			for i := range words {
				w := ^uint64(0)
				for d := 0; d < shape.densityBits; d++ {
					w &= rnd.Uint64()
				}
				words[i] = ^w
			}
			calculateAndSetCardinality(c)
			bm.setKey(uint64(k)<<16, offset)
		}
		planes[p] = bm
	}
	return planes
}

// cascadeOps alternates And/Or so density stays bounded and no pass
// short-circuits on empty or full, keeping both arms doing real work.
func cascadeOps(n int) []bool {
	ops := make([]bool, n)
	for i := range ops {
		ops[i] = i%2 == 0
	}
	return ops
}

// runCascade times the merge plus reads calls to GetCardinality; that cost
// is lazy, so it lands only on callers who ask.
func runCascade(b *testing.B, planes []*Bitmap, workers, reads int) {
	ops := cascadeOps(len(planes) - 1)
	buf := make([]byte, planes[0].LenInBytes()*2)

	b.ReportAllocs()
	b.ResetTimer()
	var sink int
	for i := 0; i < b.N; i++ {
		merged := planes[0].CloneToBuf(buf)
		for j, isAnd := range ops {
			if isAnd {
				merged.AndConc(planes[j+1], workers)
			} else {
				merged.OrConc(planes[j+1], workers)
			}
		}
		sink += merged.NumContainers()
		for r := 0; r < reads; r++ {
			sink += merged.GetCardinality()
		}
	}
	b.StopTimer()
	if sink == 0 {
		b.Fatal("cascade produced an empty result; the shape is wrong")
	}
}

// BenchmarkRangeCascade sweeps merge workers and cardinality reads: merges
// parallelize across workers, but per-container cardinality work does not.
func BenchmarkRangeCascade(b *testing.B) {
	planes := buildDensePlanes(productionCascade, 1)
	for _, workers := range []int{1, 4, 8} {
		for _, reads := range []int{0, 1, 3} {
			b.Run(fmt.Sprintf("workers=%d/reads=%d", workers, reads), func(b *testing.B) {
				runCascade(b, planes, workers, reads)
			})
		}
	}
}

// BenchmarkCorpusMerge replays the merge over real corpus data in
// testdata/bitmaps, for a realistic container mix and density. Skips if the
// corpus isn't checked out.
func BenchmarkCorpusMerge(b *testing.B) {
	for _, dataset := range corpusDatasets() {
		pairs := loadCorpusPairs(b, dataset)
		if len(pairs) < 2 {
			continue
		}
		for _, workers := range []int{1, 4, 8} {
			b.Run(fmt.Sprintf("%s/workers=%d", dataset, workers), func(b *testing.B) {
				buf := make([]byte, pairs[0].additions.LenInBytes()*4)
				b.ReportAllocs()
				b.ResetTimer()
				var sink int
				for i := 0; i < b.N; i++ {
					merged := pairs[0].additions.CloneToBuf(buf)
					for j := 1; j < len(pairs); j++ {
						if pairs[j].deletions != nil {
							merged.AndNotConc(pairs[j].deletions, workers)
						}
						if pairs[j].additions != nil {
							merged.OrConc(pairs[j].additions, workers)
						}
					}
					sink += merged.GetCardinality()
				}
				b.StopTimer()
				if sink == 0 {
					b.Fatal("merge produced an empty result")
				}
			})
		}
	}
}

func corpusDatasets() []string {
	entries, err := os.ReadDir(filepath.Join("testdata", "bitmaps"))
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

type corpusPair struct {
	additions *Bitmap
	deletions *Bitmap
}

func loadCorpusPairs(b *testing.B, dataset string) []corpusPair {
	dir := filepath.Join("testdata", "bitmaps", dataset)
	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Skipf("corpus %q not checked out: %v", dataset, err)
	}
	re := regexp.MustCompile(`^(\d+)_(additions|deletions)\.bm$`)

	byID := map[string]*corpusPair{}
	var ids []string
	for _, e := range entries {
		m := re.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		p, ok := byID[m[1]]
		if !ok {
			p = &corpusPair{}
			byID[m[1]] = p
			ids = append(ids, m[1])
		}
		content, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			b.Fatalf("read %s: %v", e.Name(), err)
		}
		bm := FromBuffer(content)
		if bm.IsEmpty() {
			continue
		}
		if m[2] == "additions" {
			p.additions = bm
		} else {
			p.deletions = bm
		}
	}
	sort.Strings(ids)

	var out []corpusPair
	for _, id := range ids {
		out = append(out, *byID[id])
	}
	if len(out) == 0 || out[0].additions == nil {
		b.Skipf("corpus %q has no seed additions", dataset)
	}
	return out
}
