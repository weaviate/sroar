package sroar

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// go test -v -bench Benchmark_MergePairs_CurrencyUSD_10 -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof -memprofile mem.prof
//
// 0.0.12 / cpu: Apple M1 Pro
// Benchmark_MergePairs_CurrencyUSD_10-10          404           2926223 ns/op           24220 B/op        192 allocs/op
// Benchmark_MergePairs_CurrencyUSD_10-10          404           2944520 ns/op           24274 B/op        192 allocs/op
// Benchmark_MergePairs_CurrencyUSD_10-10          400           2964674 ns/op           24270 B/op        192 allocs/op
func Benchmark_MergePairs_CurrencyUSD_10(b *testing.B) {
	benchmarkMergeOrAndNot(b, "currency_usd", 10)
}

// go test -v -bench Benchmark_MergePairs_CurrencyUSD_1$ -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof -memprofile mem.prof
//
// 0.0.12 / cpu: Apple M1 Pro
// Benchmark_MergePairs_CurrencyUSD_1-10                230           5256425 ns/op           19450 B/op         21 allocs/op
// Benchmark_MergePairs_CurrencyUSD_1-10                224           5261322 ns/op           19454 B/op         21 allocs/op
// Benchmark_MergePairs_CurrencyUSD_1-10                224           5258985 ns/op           19386 B/op         21 allocs/op
func Benchmark_MergePairs_CurrencyUSD_1(b *testing.B) {
	benchmarkMergeOrAndNot(b, "currency_usd", 1)
}

// go test -v -bench Benchmark_MergePairs_CurrencyEUR_10 -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof -memprofile mem.prof
//
// 0.0.12 / cpu: Apple M1 Pro
// Benchmark_MergePairs_CurrencyEUR_10-10          404           2950929 ns/op           24021 B/op        192 allocs/op
// Benchmark_MergePairs_CurrencyEUR_10-10          406           3006315 ns/op           24059 B/op        192 allocs/op
// Benchmark_MergePairs_CurrencyEUR_10-10          387           3029769 ns/op           24069 B/op        192 allocs/op
func Benchmark_MergePairs_CurrencyEUR_10(b *testing.B) {
	benchmarkMergeOrAndNot(b, "currency_eur", 10)
}

// go test -v -bench Benchmark_MergePairs_CurrencyEUR_1$ -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof -memprofile mem.prof
//
// 0.0.12 / cpu: Apple M1 Pro
// Benchmark_MergePairs_CurrencyEUR_1-10                248           4867205 ns/op           19344 B/op         20 allocs/op
// Benchmark_MergePairs_CurrencyEUR_1-10                247           4752302 ns/op           19344 B/op         20 allocs/op
// Benchmark_MergePairs_CurrencyEUR_1-10                250           4733349 ns/op           19344 B/op         20 allocs/op
func Benchmark_MergePairs_CurrencyEUR_1(b *testing.B) {
	benchmarkMergeOrAndNot(b, "currency_eur", 1)
}

// go test -v -bench Benchmark_MergePairs_RetailerWalmart_10 -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof -memprofile mem.prof
//
// 0.0.12 / cpu: Apple M1 Pro
// Benchmark_MergePairs_RetailerWalmart_10-10             5652            205146 ns/op             512 B/op         12 allocs/op
// Benchmark_MergePairs_RetailerWalmart_10-10             5912            196649 ns/op             523 B/op         12 allocs/op
// Benchmark_MergePairs_RetailerWalmart_10-10             6054            195562 ns/op             518 B/op         12 allocs/op
func Benchmark_MergePairs_RetailerWalmart_10(b *testing.B) {
	benchmarkMergeOrAndNot(b, "retailer_walmart", 10)
}

// go test -v -bench Benchmark_MergePairs_RetailerWalmart_1$ -benchmem -run ^$ github.com/weaviate/sroar -cpuprofile cpu.prof -memprofile mem.prof
//
// 0.0.12 / cpu: Apple M1 Pro
// Benchmark_MergePairs_RetailerWalmart_1-10           2292            460649 ns/op              64 B/op          2 allocs/op
// Benchmark_MergePairs_RetailerWalmart_1-10           2504            465026 ns/op              64 B/op          2 allocs/op
// Benchmark_MergePairs_RetailerWalmart_1-10           2432            449451 ns/op              64 B/op          2 allocs/op
func Benchmark_MergePairs_RetailerWalmart_1(b *testing.B) {
	benchmarkMergeOrAndNot(b, "retailer_walmart", 1)
}

func benchmarkMergeOrAndNot(b *testing.B, bitmapsDataset string, concurrency int) {
	pairs, _ := loadBitmaps(bitmapsDataset)
	ln := pairs[0].additions.LenInBytes()
	buf := make([]byte, ln*5/4) // + 25%

	for b.Loop() {
		b.StopTimer()
		bm := pairs[0].additions.CloneToBuf(buf)
		b.StartTimer()

		for j := 1; j < len(pairs); j++ {
			bm.AndNotConc(pairs[j].deletions, concurrency)
			bm.OrConc(pairs[j].additions, concurrency)
		}
	}
}

func TestBitmapRead(t *testing.T) {
	pairs, err := loadBitmaps("currency_jpy")
	require.NoError(t, err)

	require.Len(t, pairs, 7)
	require.Nil(t, pairs[0].deletions)
	require.NotNil(t, pairs[0].additions)
	for i := 1; i < len(pairs); i++ {
		require.NotNil(t, pairs[i].deletions)
		require.NotNil(t, pairs[i].additions)
	}
}

func TestBitmapFile_Merge_OrAndNot(t *testing.T) {
	pairs, _ := loadBitmaps("currency_jpy")

	bm := pairs[0].additions.Clone()
	for j := 1; j < len(pairs); j++ {
		bm.AndNotConc(pairs[j].deletions, 10)
		bm.OrConc(pairs[j].additions, 10)
	}

	require.Equal(t, 209879, bm.GetCardinality())
	require.Equal(t, uint64(69239), bm.Minimum())
	require.Equal(t, uint64(446423875), bm.Maximum())
}

// -----------------------------------------------------------------------------

type pair struct {
	additions *Bitmap
	deletions *Bitmap
	addfile   string
	delfile   string
}

func loadBitmaps(dataset string) ([]pair, error) {
	dirpath := filepath.Join("testdata", "bitmaps", dataset)
	entries, err := os.ReadDir(dirpath)
	if err != nil {
		return nil, fmt.Errorf("read bitmaps dir %q: %w", dirpath, err)
	}

	lastId := int64(-1)
	pairs := []pair{}

	r := regexp.MustCompile(`^(\d+)_(additions|deletions).bm$`)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !r.MatchString(entry.Name()) {
			return nil, fmt.Errorf("invalid name format %q, expected \"02_additions.bm\" or \"03_deletions.bm\"", entry.Name())
		}

		submatch := r.FindStringSubmatch(entry.Name())
		idstr, bmtype := submatch[1], submatch[2]
		id, _ := strconv.ParseInt(idstr, 10, 8)

		content, err := os.ReadFile(filepath.Join(dirpath, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read bitmap file %q: %w", entry.Name(), err)
		}

		bm := FromBuffer(content)

		if lastId != id {
			pairs = append(pairs, pair{})
			lastId = id
		}
		p := &pairs[len(pairs)-1]

		switch bmtype {
		case "additions":
			p.additions = bm
			p.addfile = entry.Name()
		case "deletions":
			p.deletions = bm
			p.delfile = entry.Name()
		}
	}

	return pairs, nil
}
