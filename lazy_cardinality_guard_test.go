package sroar

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A container's cardinality header is not always exact: the in-place merges
// defer it (see runLazy), so getCardinality can answer invalidCardinality.
// Reading that value as if it were a count is a wrong answer or a panic, not a
// slowdown, and nothing in the type system stops it.
//
// TestNoUnclassifiedCardinalityObserver closes that hole statically. It finds
// every exported function and method whose call graph reaches getCardinality
// and requires each one to appear in cardinalityObservers below with how it
// stays correct. A new exported entry point that reads a cardinality — directly
// or through any unexported helper — fails this test until its author says
// which case it is.
//
// The classification is a claim about the code, not a suppression: the
// behavioural counterpart is TestLazyCardinality_ObservationParity, which
// executes these same entry points against a settled reference.
type cardinalityHandling string

const (
	// settlesExactly recounts, or writes back, before using the value as a
	// count.
	settlesExactly cardinalityHandling = "settles the header exactly"
	// branchesOnExactAnswers only ever compares the value against 0 or
	// maxCardinality — which a deferred header still answers exactly, see
	// lazyCardinality — or against a density threshold that selects a loop
	// shape and cannot change the result.
	branchesOnExactAnswers cardinalityHandling = "branches only on answers a deferred header keeps exact"
	// boundedEstimate uses the value as an upper bound for sizing or ordering,
	// where the deferred header's worst case is already the assumption.
	boundedEstimate cardinalityHandling = "uses a worst-case bound only"
	// arrayContainersOnly reads the header of a container that is never a
	// bitmap, and only bitmap containers ever defer.
	arrayContainersOnly cardinalityHandling = "reads array containers only"
	// buildsFreshContainers only reads headers it wrote itself in the same
	// call, so it never meets a deferred one.
	buildsFreshContainers cardinalityHandling = "reads only headers it just wrote"
)

var cardinalityObservers = map[string]cardinalityHandling{
	// --- settle exactly -----------------------------------------------------
	"Bitmap.GetCardinality":   settlesExactly,
	"Bitmap.ToArray":          settlesExactly,
	"Bitmap.Select":           settlesExactly,
	"Bitmap.Rank":             settlesExactly,
	"Bitmap.String":           settlesExactly,
	"Bitmap.ToBuffer":         settlesExactly,
	"Bitmap.ToBufferWithCopy": settlesExactly,
	"Bitmap.Clone":            settlesExactly,
	"Bitmap.CloneToBuf":       settlesExactly,
	"Bitmap.Split":            settlesExactly,
	"Bitmap.Set":              settlesExactly,
	"Bitmap.SetMany":          settlesExactly,
	"Bitmap.Remove":           settlesExactly,
	"Bitmap.RemoveRange":      settlesExactly,
	"Bitmap.FillUp":           settlesExactly,
	"Bitmap.Debug":            settlesExactly,
	"Iterator.Next":           settlesExactly,
	"ManyItr.NextMany":        settlesExactly,
	"Bitmap.ManyIterator":     settlesExactly,
	"AndNot":                  settlesExactly,
	"Bitmap.AndNot":           settlesExactly,
	"Bitmap.AndNotConc":       settlesExactly,
	"Bitmap.Or":               settlesExactly,
	"Bitmap.OrConc":           settlesExactly,
	"Bitmap.OrOld":            settlesExactly,
	"Or":                      settlesExactly,
	"OrOld":                   settlesExactly,
	"Bitmap.AndOld":           settlesExactly,
	"Bitmap.AndNotOld":        settlesExactly,
	"AndOld":                  settlesExactly,
	"Bitmap.Masked":           settlesExactly,
	"Bitmap.MaskedToBuf":      settlesExactly,
	"MaskedAnd":               settlesExactly,
	"MaskedAndToBuf":          settlesExactly,
	"Bitmap.AndMasked":        settlesExactly,
	"Bitmap.AndMaskedConc":    settlesExactly,
	"CopresenceByMask":        settlesExactly,
	"CopresenceByMaskToBuf":   settlesExactly,
	"FromSortedList":          buildsFreshContainers,
	"Prefill":                 buildsFreshContainers,

	// --- branch only on answers a deferred header keeps exact ---------------
	"Bitmap.IsEmpty":                   branchesOnExactAnswers,
	"Bitmap.Minimum":                   branchesOnExactAnswers,
	"Bitmap.Maximum":                   branchesOnExactAnswers,
	"Bitmap.Cleanup":                   branchesOnExactAnswers,
	"Bitmap.And":                       branchesOnExactAnswers,
	"Bitmap.AndConc":                   branchesOnExactAnswers,
	"And":                              branchesOnExactAnswers,
	"Bitmap.Intersects":                branchesOnExactAnswers,
	"Bitmap.IntersectsMasked":          branchesOnExactAnswers,
	"Bitmap.ConvertToBitmapContainers": branchesOnExactAnswers,
	"Bitmap.ZeroOut":                   branchesOnExactAnswers,
	"Bitmap.NewRangeIterators":         branchesOnExactAnswers,

	// --- bounded estimates --------------------------------------------------
	"FastOr":    boundedEstimate,
	"FastAnd":   boundedEstimate,
	"FastParOr": boundedEstimate,

	// --- array containers only ---------------------------------------------
	"Bitmap.Contains":          arrayContainersOnly,
	"ContainsCursor.Contains":  arrayContainersOnly,
	"ContainsCursor.NextGeq":   arrayContainersOnly,
	"ContainsCursor.Reset":     arrayContainersOnly,
	"Bitmap.NewContainsCursor": arrayContainersOnly,
	"Bitmap.NewIterator":       arrayContainersOnly,
}

// cardinalityReaders are the leaves that read a cardinality header. Any
// function whose call graph reaches one of these is an observer.
var cardinalityReaders = map[string]bool{
	"getCardinality": true,
	"isEmpty":        true,
}

func TestNoUnclassifiedCardinalityObserver(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var sources []string
	for _, p := range paths {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		src, err := os.ReadFile(p)
		require.NoError(t, err)
		sources = append(sources, string(src))
	}

	declared, observers := cardinalityObserverAnalysis(t, sources)

	var unclassified []string
	for _, name := range observers {
		if _, ok := cardinalityObservers[name]; !ok {
			unclassified = append(unclassified, name)
		}
	}
	sort.Strings(unclassified)
	require.Emptyf(t, unclassified,
		"these exported entry points read a container cardinality but are not "+
			"classified in cardinalityObservers: %v\n\n"+
			"A cardinality header can be deferred (invalidCardinality) after an "+
			"in-place merge. Decide how each one stays correct — settle it via "+
			"containerCardinality/reconcileCardinality, test only empty/full, or "+
			"use cardinalityUpperBound — then record the choice here and add the "+
			"entry point to TestLazyCardinality_ObservationParity.", unclassified)

	// The manifest must not rot in the other direction either.
	var stale []string
	for name := range cardinalityObservers {
		if !declared[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	require.Emptyf(t, stale,
		"cardinalityObservers names functions that no longer exist: %v", stale)
}

// TestCardinalityGuardDetectsARogueObserver runs the analysis over a synthetic
// package whose exported function reaches getCardinality only through an
// unexported helper. Without this the guard above could report an empty list
// because the walk is broken rather than because the code is clean.
func TestCardinalityGuardDetectsARogueObserver(t *testing.T) {
	sources := []string{`package sroar

// reachable only transitively, and through a method, so a one-level check
// or a function-only check both miss it
func RogueObserver(bm *Bitmap) int { return bm.rogueStep() }

func (bm *Bitmap) rogueStep() int { return rogueLeaf(bm) }

func rogueLeaf(bm *Bitmap) int { return getCardinality(bm.data) }

// exported but clean: must not be reported
func CleanEntryPoint(bm *Bitmap) int { return len(bm.data) }

// reaches a reader but is unexported: must not be reported
func hiddenObserver(bm *Bitmap) int { return rogueLeaf(bm) }
`}

	_, observers := cardinalityObserverAnalysis(t, sources)
	require.Equal(t, []string{"RogueObserver"}, observers)
}

// cardinalityObserverAnalysis parses sources as one package and returns every
// declared function key, plus the sorted keys of those reachable from outside
// the package whose call graph reaches a cardinality reader.
func cardinalityObserverAnalysis(t *testing.T, sources []string) (map[string]bool, []string) {
	t.Helper()
	fset := token.NewFileSet()

	calls := map[string]map[string]bool{}
	entryPoint := map[string]bool{}

	for i, src := range sources {
		file, err := parser.ParseFile(fset, filepath.Join("src", string(rune('a'+i%26))+".go"), src, 0)
		require.NoError(t, err)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name == nil {
				continue
			}
			name := funcKey(fn)
			if _, seen := calls[name]; !seen {
				calls[name] = map[string]bool{}
			}
			entryPoint[name] = isEntryPoint(fn)
			ast.Inspect(fn, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch f := call.Fun.(type) {
				case *ast.Ident:
					calls[name][f.Name] = true
				case *ast.SelectorExpr:
					// a method call gives only the method name; it is resolved
					// against every declaration of that name below
					calls[name][f.Sel.Name] = true
				}
				return true
			})
		}
	}

	// Linking a method call to every declaration of that name over-approximates
	// the call graph. That is the safe direction: it can demand a
	// classification that is not strictly needed, never miss one that is.
	byMethodName := map[string][]string{}
	for name := range calls {
		if i := strings.IndexByte(name, '.'); i >= 0 {
			byMethodName[name[i+1:]] = append(byMethodName[name[i+1:]], name)
		}
	}

	memo := map[string]bool{}
	var reaches func(name string, onPath map[string]bool) bool
	reaches = func(name string, onPath map[string]bool) bool {
		if v, done := memo[name]; done {
			return v
		}
		if onPath[name] {
			return false // recursion adds nothing on this path
		}
		onPath[name] = true
		defer delete(onPath, name)

		for callee := range calls[name] {
			if cardinalityReaders[callee] {
				memo[name] = true
				return true
			}
			if _, declared := calls[callee]; declared && reaches(callee, onPath) {
				memo[name] = true
				return true
			}
			for _, m := range byMethodName[callee] {
				if reaches(m, onPath) {
					memo[name] = true
					return true
				}
			}
		}
		return false
	}

	declared := make(map[string]bool, len(calls))
	var observers []string
	for name := range calls {
		declared[name] = true
		if entryPoint[name] && reaches(name, map[string]bool{}) {
			observers = append(observers, name)
		}
	}
	sort.Strings(observers)
	return declared, observers
}

// funcKey names a declaration as "Func" or "Receiver.Method".
func funcKey(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return receiverTypeName(fn.Recv.List[0].Type) + "." + fn.Name.Name
}

func receiverTypeName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return "?"
}

// isEntryPoint reports whether a declaration is reachable from outside the
// package: an exported function, or an exported method on an exported type.
func isEntryPoint(fn *ast.FuncDecl) bool {
	if !fn.Name.IsExported() {
		return false
	}
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return true
	}
	return ast.IsExported(receiverTypeName(fn.Recv.List[0].Type))
}
