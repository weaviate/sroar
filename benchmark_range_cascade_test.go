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

// The benchmarks here model the merge a bit-sliced numeric range filter drives:
// clone the widest plane, then fold a dozen or so more planes into it with
// alternating AndConc/OrConc. Every pass is a whole-bitmap sweep, and the
// cardinality each pass computes is overwritten by the next one.
//
// The timed region ends with GetCardinality, so the cost of settling every
// deferred header is charged to the result rather than left out of it.
//
// This file deliberately uses nothing that does not already exist on the
// released code, so the same benchmark can be run in a pristine checkout to
// produce the baseline.

// cascadeShape describes the production-shaped operand set: 369 bitmap
// containers per plane at ~24.2M values, matching a single shard's range planes.
type cascadeShape struct {
	containers int
	planes     int
	// densityBits sets each plane's fill to 1 - 2^-densityBits. Chosen high
	// enough that containers stay bitmap-typed and neither empty nor full, so
	// every pass does real work.
	densityBits int
}

var productionCascade = cascadeShape{containers: 369, planes: 15, densityBits: 4}

// buildDensePlanes writes bitmap containers directly rather than going through
// Set, which would need 24M calls per plane.
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
				// AND of densityBits random words clears a bit with
				// probability 2^-densityBits
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

// cascadeOps is the And/Or sequence. Alternating keeps density bounded so no
// pass gets skipped by an empty or full short-circuit, which would make the
// two arms do different amounts of work.
func cascadeOps(n int) []bool {
	ops := make([]bool, n)
	for i := range ops {
		ops[i] = i%2 == 0
	}
	return ops
}

// runCascade folds the planes and then reads the result's cardinality reads
// times. reads matters: a caller that never asks for a cardinality pays nothing
// for one, and a caller that asks repeatedly pays each time.
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

// BenchmarkRangeCascade sweeps merge workers against the number of times the
// caller reads the result's cardinality. Both axes matter: the merge fans out
// across workers while any per-container cardinality work the read triggers
// does not.
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

// BenchmarkCorpusMerge replays the merge the recorded production bitmaps under
// testdata/bitmaps were captured for: fold each segment's deletions out and its
// additions in. Unlike the cascade above it is not a nested plane set — the
// per-segment additions are disjoint, so an AND across them is empty — but it
// is the real container mix and density, and OrConc is half of it. It skips
// when the corpus is not checked out.
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
