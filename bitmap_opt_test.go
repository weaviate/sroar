package sroar

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareMergeImplementations(t *testing.T) {
	randSeed := int64(1724861525311)
	rnd := rand.New(rand.NewSource(randSeed))
	maxConcurrency := 4
	bufs := make([][]uint16, maxConcurrency)
	for i := range bufs {
		bufs[i] = make([]uint16, maxContainerSize)
	}

	NA := 1024  // fits array container
	NB := 16384 // fits bitmap container

	bmA := NewBitmap()      // array + array + bitmap + bitmap
	bmB := NewBitmap()      // array + bitmap + array + bitmap
	bmC := NewBitmap()      // array + bitmap + bitmap + array
	bmD := NewBitmap()      // bitmap + array + array + bitmap
	bmE := NewBitmap()      // bitmap + array + bitmap + array
	bmF := NewBitmap()      // bitmap + bitmap + array + array
	bigA := NewBitmap()     // just arrays
	bigB := NewBitmap()     // just bitmaps
	superset := NewBitmap() //all

	randInRange := func(rng uint64) uint64 {
		return uint64(rnd.Int31n(int32(maxCardinality))) + rng*uint64(maxCardinality)
	}

	t.Run("populate", func(t *testing.T) {
		var a, b, c, d, e, f uint64

		for i := 0; i < NA; i++ {
			a = randInRange(0)
			b = randInRange(0)
			c = randInRange(0)

			bmA.Set(a)
			bmB.Set(b)
			bmC.Set(c)
			bigA.Set(a)
			bigA.Set(b)
			bigA.Set(c)
			superset.Set(a)
			superset.Set(b)
			superset.Set(c)

			a = randInRange(1)
			d = randInRange(1)
			e = randInRange(1)

			bmA.Set(a)
			bmD.Set(d)
			bmE.Set(e)
			bigA.Set(a)
			bigA.Set(d)
			bigA.Set(e)
			superset.Set(a)
			superset.Set(d)
			superset.Set(e)

			b = randInRange(2)
			d = randInRange(2)
			f = randInRange(2)

			bmB.Set(b)
			bmD.Set(d)
			bmF.Set(f)
			bigA.Set(b)
			bigA.Set(d)
			bigA.Set(f)
			superset.Set(b)
			superset.Set(d)
			superset.Set(f)

			c = randInRange(3)
			e = randInRange(3)
			f = randInRange(3)

			bmC.Set(c)
			bmE.Set(e)
			bmF.Set(f)
			bigA.Set(c)
			bigA.Set(e)
			bigA.Set(f)
			superset.Set(c)
			superset.Set(e)
			superset.Set(f)
		}

		for i := 0; i < NB; i++ {
			d = randInRange(0)
			e = randInRange(0)
			f = randInRange(0)

			bmD.Set(d)
			bmE.Set(e)
			bmF.Set(f)
			bigB.Set(d)
			bigB.Set(e)
			bigB.Set(f)
			superset.Set(d)
			superset.Set(e)
			superset.Set(f)

			b = randInRange(1)
			c = randInRange(1)
			f = randInRange(1)

			bmB.Set(b)
			bmC.Set(c)
			bmF.Set(f)
			bigB.Set(b)
			bigB.Set(c)
			bigB.Set(f)
			superset.Set(b)
			superset.Set(c)
			superset.Set(f)

			a = randInRange(2)
			c = randInRange(2)
			e = randInRange(2)

			bmA.Set(a)
			bmC.Set(c)
			bmE.Set(e)
			bigB.Set(a)
			bigB.Set(c)
			bigB.Set(e)
			superset.Set(a)
			superset.Set(c)
			superset.Set(e)

			a = randInRange(3)
			b = randInRange(3)
			d = randInRange(3)

			bmA.Set(a)
			bmB.Set(b)
			bmD.Set(d)
			bigB.Set(a)
			bigB.Set(b)
			bigB.Set(d)
			superset.Set(a)
			superset.Set(b)
			superset.Set(d)
		}
	})

	t.Run("and", func(t *testing.T) {
		run := func(t *testing.T, dst, src *Bitmap, expCardinality int, match bool) {
			and1 := dst.Clone()

			and1.AndOld(src)
			and2 := dst.Clone().And(src)
			and4 := dst.Clone().AndConc(src, maxConcurrency)
			and6 := AndOld(dst, src)
			and7 := And(dst, src)

			require.Equal(t, expCardinality, and1.GetCardinality())
			if match {
				assertMatches(t, and1, and2, and4, and6, and7)
			} else {
				require.Equal(t, expCardinality, and2.GetCardinality())
				require.Equal(t, expCardinality, and4.GetCardinality())
				require.Equal(t, expCardinality, and6.GetCardinality())
				require.Equal(t, expCardinality, and7.GetCardinality())
			}
		}
		runMatch := func(t *testing.T, dst, src *Bitmap, expCardinality int) {
			run(t, dst, src, expCardinality, true)
		}
		runNoMatch := func(t *testing.T, dst, src *Bitmap, expCardinality int) {
			run(t, dst, src, expCardinality, false)
		}

		runNoMatch(t, bmA, bmB, 3675)
		runNoMatch(t, bmA, bmC, 3693)
		runNoMatch(t, bmA, bmD, 3627)
		runMatch(t, bmA, bmE, 3730)
		runNoMatch(t, bmA, bmF, 932)

		runNoMatch(t, bmB, bmA, 3675)
		runMatch(t, bmB, bmC, 3689)
		runNoMatch(t, bmB, bmD, 3676)
		runNoMatch(t, bmB, bmE, 882)
		runNoMatch(t, bmB, bmF, 3601)

		runNoMatch(t, bmC, bmA, 3693)
		runNoMatch(t, bmC, bmB, 3689)
		runNoMatch(t, bmC, bmD, 928)
		runMatch(t, bmC, bmE, 3701)
		runNoMatch(t, bmC, bmF, 3610)

		runNoMatch(t, bmD, bmA, 3627)
		runMatch(t, bmD, bmB, 3676)
		runNoMatch(t, bmD, bmC, 928)
		runNoMatch(t, bmD, bmE, 3666)
		runNoMatch(t, bmD, bmF, 3654)

		runNoMatch(t, bmE, bmA, 3730)
		runNoMatch(t, bmE, bmB, 882)
		runNoMatch(t, bmE, bmC, 3701)
		runNoMatch(t, bmE, bmD, 3666)
		runMatch(t, bmE, bmF, 3674)

		runNoMatch(t, bmF, bmA, 932)
		runNoMatch(t, bmF, bmB, 3601)
		runNoMatch(t, bmF, bmC, 3610)
		runMatch(t, bmF, bmD, 3654)
		runNoMatch(t, bmF, bmE, 3674)

		runNoMatch(t, superset, bmA, 31006)
		runNoMatch(t, superset, bmB, 30995)
		runNoMatch(t, superset, bmC, 31015)
		runNoMatch(t, superset, bmD, 31091)
		runNoMatch(t, superset, bmE, 30967)
		runNoMatch(t, superset, bmF, 31085)

		runNoMatch(t, bmA, superset, 31006)
		runNoMatch(t, bmB, superset, 30995)
		runNoMatch(t, bmC, superset, 31015)
		runNoMatch(t, bmD, superset, 31091)
		runNoMatch(t, bmE, superset, 30967)
		runNoMatch(t, bmF, superset, 31085)

		runNoMatch(t, bigA, bmA, 3407)
		runNoMatch(t, bigA, bmB, 3349)
		runNoMatch(t, bigA, bmC, 3307)
		runNoMatch(t, bigA, bmD, 3360)
		runNoMatch(t, bigA, bmE, 3413)
		runNoMatch(t, bigA, bmF, 3331)

		runNoMatch(t, bmA, bigA, 3407)
		runNoMatch(t, bmB, bigA, 3349)
		runNoMatch(t, bmC, bigA, 3307)
		runNoMatch(t, bmD, bigA, 3360)
		runNoMatch(t, bmE, bigA, 3413)
		runNoMatch(t, bmF, bigA, 3331)

		runNoMatch(t, bigB, bmA, 30061)
		runNoMatch(t, bigB, bmB, 30006)
		runNoMatch(t, bigB, bmC, 30092)
		runNoMatch(t, bigB, bmD, 30154)
		runNoMatch(t, bigB, bmE, 29996)
		runNoMatch(t, bigB, bmF, 30097)

		runNoMatch(t, bmA, bigB, 30061)
		runNoMatch(t, bmB, bigB, 30006)
		runNoMatch(t, bmC, bigB, 30092)
		runNoMatch(t, bmD, bigB, 30154)
		runNoMatch(t, bmE, bigB, 29996)
		runNoMatch(t, bmF, bigB, 30097)
	})

	t.Run("andNot", func(t *testing.T) {
		run := func(t *testing.T, dst, src *Bitmap, expCardinality int, match bool) {
			andNot1 := dst.Clone()

			andNot1.AndNotOld(src)
			andNot2 := dst.Clone().AndNot(src)
			andNot4 := dst.Clone().AndNotConc(src, maxConcurrency)
			andNot6 := AndNot(dst, src)

			require.Equal(t, expCardinality, andNot1.GetCardinality())
			if match {
				assertMatches(t, andNot1, andNot2, andNot4, andNot6)
			} else {
				require.Equal(t, expCardinality, andNot2.GetCardinality())
				require.Equal(t, expCardinality, andNot4.GetCardinality())
				require.Equal(t, expCardinality, andNot6.GetCardinality())
			}
		}
		runMatch := func(t *testing.T, dst, src *Bitmap, expCardinality int) {
			run(t, dst, src, expCardinality, true)
		}
		runNoMatch := func(t *testing.T, dst, src *Bitmap, expCardinality int) {
			run(t, dst, src, expCardinality, false)
		}

		runNoMatch(t, bmA, bmB, 27331)
		runNoMatch(t, bmA, bmC, 27313)
		runNoMatch(t, bmA, bmD, 27379)
		runNoMatch(t, bmA, bmE, 27276)
		runMatch(t, bmA, bmF, 30074)

		runNoMatch(t, bmB, bmA, 27320)
		runNoMatch(t, bmB, bmC, 27306)
		runNoMatch(t, bmB, bmD, 27319)
		runMatch(t, bmB, bmE, 30113)
		runNoMatch(t, bmB, bmF, 27394)

		runNoMatch(t, bmC, bmA, 27322)
		runNoMatch(t, bmC, bmB, 27326)
		runMatch(t, bmC, bmD, 30087)
		runNoMatch(t, bmC, bmE, 27314)
		runNoMatch(t, bmC, bmF, 27405)

		runNoMatch(t, bmD, bmA, 27464)
		runNoMatch(t, bmD, bmB, 27415)
		runMatch(t, bmD, bmC, 30163)
		runNoMatch(t, bmD, bmE, 27425)
		runNoMatch(t, bmD, bmF, 27437)

		runNoMatch(t, bmE, bmA, 27237)
		runMatch(t, bmE, bmB, 30085)
		runNoMatch(t, bmE, bmC, 27266)
		runNoMatch(t, bmE, bmD, 27301)
		runNoMatch(t, bmE, bmF, 27293)

		runMatch(t, bmF, bmA, 30153)
		runNoMatch(t, bmF, bmB, 27484)
		runNoMatch(t, bmF, bmC, 27475)
		runNoMatch(t, bmF, bmD, 27431)
		runNoMatch(t, bmF, bmE, 27411)

		runNoMatch(t, superset, bmA, 112986)
		runNoMatch(t, superset, bmB, 112997)
		runNoMatch(t, superset, bmC, 112977)
		runNoMatch(t, superset, bmD, 112901)
		runNoMatch(t, superset, bmE, 113025)
		runNoMatch(t, superset, bmF, 112907)

		runNoMatch(t, bmA, superset, 0)
		runNoMatch(t, bmB, superset, 0)
		runNoMatch(t, bmC, superset, 0)
		runNoMatch(t, bmD, superset, 0)
		runNoMatch(t, bmE, superset, 0)
		runNoMatch(t, bmF, superset, 0)

		runNoMatch(t, bigA, bmA, 8613)
		runNoMatch(t, bigA, bmB, 8671)
		runNoMatch(t, bigA, bmC, 8713)
		runNoMatch(t, bigA, bmD, 8660)
		runNoMatch(t, bigA, bmE, 8607)
		runNoMatch(t, bigA, bmF, 8689)

		runNoMatch(t, bmA, bigA, 27599)
		runNoMatch(t, bmB, bigA, 27646)
		runNoMatch(t, bmC, bigA, 27708)
		runNoMatch(t, bmD, bigA, 27731)
		runNoMatch(t, bmE, bigA, 27554)
		runNoMatch(t, bmF, bigA, 27754)

		runNoMatch(t, bigB, bmA, 108261)
		runNoMatch(t, bigB, bmB, 108316)
		runNoMatch(t, bigB, bmC, 108230)
		runNoMatch(t, bigB, bmD, 108168)
		runNoMatch(t, bigB, bmE, 108326)
		runNoMatch(t, bigB, bmF, 108225)

		runNoMatch(t, bmA, bigB, 945)
		runNoMatch(t, bmB, bigB, 989)
		runNoMatch(t, bmC, bigB, 923)
		runNoMatch(t, bmD, bigB, 937)
		runNoMatch(t, bmE, bigB, 971)
		runNoMatch(t, bmF, bigB, 988)
	})

	t.Run("or", func(t *testing.T) {
		run := func(t *testing.T, dst, src *Bitmap, expCardinality int, match bool) {
			or1 := dst.Clone()

			or1.OrOld(src)
			or2 := dst.Clone().Or(src)
			or4 := dst.Clone().OrConc(src, maxConcurrency)
			or6 := OrOld(dst, src)
			or7 := Or(dst, src)

			require.Equal(t, expCardinality, or1.GetCardinality())
			if match {
				assertMatches(t, or1, or2, or4, or6, or7)
			} else {
				require.Equal(t, expCardinality, or2.GetCardinality())
				require.Equal(t, expCardinality, or4.GetCardinality())
				require.Equal(t, expCardinality, or6.GetCardinality())
				require.Equal(t, expCardinality, or7.GetCardinality())
			}
		}
		runMatch := func(t *testing.T, dst, src *Bitmap, expCardinality int) {
			run(t, dst, src, expCardinality, true)
		}
		runNoMatch := func(t *testing.T, dst, src *Bitmap, expCardinality int) {
			run(t, dst, src, expCardinality, false)
		}

		runNoMatch(t, bmA, bmB, 58326)
		runNoMatch(t, bmA, bmC, 58328)
		runNoMatch(t, bmA, bmD, 58470)
		runNoMatch(t, bmA, bmE, 58243)
		runMatch(t, bmA, bmF, 61159)

		runNoMatch(t, bmB, bmA, 58326)
		runNoMatch(t, bmB, bmC, 58321)
		runNoMatch(t, bmB, bmD, 58410)
		runMatch(t, bmB, bmE, 61080)
		runNoMatch(t, bmB, bmF, 58479)

		runNoMatch(t, bmC, bmA, 58328)
		runNoMatch(t, bmC, bmB, 58321)
		runMatch(t, bmC, bmD, 61178)
		runNoMatch(t, bmC, bmE, 58281)
		runNoMatch(t, bmC, bmF, 58490)

		runMatch(t, bmD, bmA, 58470)
		runNoMatch(t, bmD, bmB, 58410)
		runNoMatch(t, bmD, bmC, 61178)
		runNoMatch(t, bmD, bmE, 58392)
		runNoMatch(t, bmD, bmF, 58522)

		runNoMatch(t, bmE, bmA, 58243)
		runNoMatch(t, bmE, bmB, 61080)
		runNoMatch(t, bmE, bmC, 58281)
		runMatch(t, bmE, bmD, 58392)
		runNoMatch(t, bmE, bmF, 58378)

		runNoMatch(t, bmF, bmA, 61159)
		runNoMatch(t, bmF, bmB, 58479)
		runNoMatch(t, bmF, bmC, 58490)
		runMatch(t, bmF, bmD, 58522)
		runNoMatch(t, bmF, bmE, 58378)

		runNoMatch(t, superset, bmA, 143992)
		runNoMatch(t, superset, bmB, 143992)
		runNoMatch(t, superset, bmC, 143992)
		runNoMatch(t, superset, bmD, 143992)
		runNoMatch(t, superset, bmE, 143992)
		runNoMatch(t, superset, bmF, 143992)

		runNoMatch(t, bmA, superset, 143992)
		runNoMatch(t, bmB, superset, 143992)
		runNoMatch(t, bmC, superset, 143992)
		runNoMatch(t, bmD, superset, 143992)
		runNoMatch(t, bmE, superset, 143992)
		runNoMatch(t, bmF, superset, 143992)

		runNoMatch(t, bigA, bmA, 39619)
		runNoMatch(t, bigA, bmB, 39666)
		runNoMatch(t, bigA, bmC, 39728)
		runNoMatch(t, bigA, bmD, 39751)
		runNoMatch(t, bigA, bmE, 39574)
		runNoMatch(t, bigA, bmF, 39774)

		runNoMatch(t, bmA, bigA, 39619)
		runNoMatch(t, bmB, bigA, 39666)
		runNoMatch(t, bmC, bigA, 39728)
		runNoMatch(t, bmD, bigA, 39751)
		runNoMatch(t, bmE, bigA, 39574)
		runNoMatch(t, bmF, bigA, 39774)

		runNoMatch(t, bigB, bmA, 139267)
		runNoMatch(t, bigB, bmB, 139311)
		runNoMatch(t, bigB, bmC, 139245)
		runNoMatch(t, bigB, bmD, 139259)
		runNoMatch(t, bigB, bmE, 139293)
		runNoMatch(t, bigB, bmF, 139310)

		runNoMatch(t, bmA, bigB, 139267)
		runNoMatch(t, bmB, bigB, 139311)
		runNoMatch(t, bmC, bigB, 139245)
		runNoMatch(t, bmD, bigB, 139259)
		runNoMatch(t, bmE, bigB, 139293)
		runNoMatch(t, bmF, bigB, 139310)
	})

	t.Run("sequence", func(t *testing.T) {
		run := func(t *testing.T, dst, a, b, c, d, e, f *Bitmap, expCardinality int, match bool) {
			seq1 := dst.Clone()
			var seq6, seq7 *Bitmap

			seq1.OrOld(a)
			seq1.AndOld(b)
			seq1.AndNotOld(c)
			seq1.OrOld(d)
			seq1.AndOld(e)
			seq1.AndNotOld(f)

			seq2 := dst.Clone().Or(a).And(b).AndNot(c).Or(d).And(e).AndNot(f)

			seq4 := dst.Clone().OrConc(a, maxConcurrency).AndConc(b, maxConcurrency).AndNotConc(c, maxConcurrency).
				OrConc(d, maxConcurrency).AndConc(e, maxConcurrency).AndNotConc(f, maxConcurrency)

			seq6 = OrOld(dst, a)
			seq6 = AndOld(seq6, b)
			seq6.AndNotOld(c)
			seq6 = OrOld(seq6, d)
			seq6 = AndOld(seq6, e)
			seq6.AndNotOld(f)

			seq7 = Or(dst, a)
			seq7 = And(seq7, b)
			seq7 = AndNot(seq7, c)
			seq7 = Or(seq7, d)
			seq7 = And(seq7, e)
			seq7 = AndNot(seq7, f)

			require.Equal(t, expCardinality, seq1.GetCardinality())
			if match {
				assertMatches(t, seq1, seq2, seq4, seq6, seq7)
			} else {
				require.Equal(t, expCardinality, seq2.GetCardinality())
				require.Equal(t, expCardinality, seq4.GetCardinality())
				require.Equal(t, expCardinality, seq6.GetCardinality())
				require.Equal(t, expCardinality, seq7.GetCardinality())
			}
		}

		runMatch := func(t *testing.T, dst, a, b, c, d, e, f *Bitmap, expCardinality int) {
			run(t, dst, a, b, c, d, e, f, expCardinality, true)
		}
		runNoMatch := func(t *testing.T, dst, a, b, c, d, e, f *Bitmap, expCardinality int) {
			run(t, dst, a, b, c, d, e, f, expCardinality, false)
		}

		runMatch(t, bmA, bmB, bmC, bmD, bmE, bmF, bigA, 3729)
		runMatch(t, bmB, bmC, bmD, bmE, bmF, bigA, bigB, 993)
		runMatch(t, bmC, bmD, bmE, bmF, bigA, bigB, superset, 0)
		runNoMatch(t, bmD, bmE, bmF, bigA, bigB, superset, bmA, 108261)
		runNoMatch(t, bmE, bmF, bigA, bigB, superset, bmA, bmB, 27331)
		runMatch(t, bmF, bigA, bigB, superset, bmA, bmB, bmC, 3545)
		runNoMatch(t, bigA, bigB, superset, bmA, bmB, bmC, bmD, 26621)
		runMatch(t, bigB, superset, bmA, bmB, bmC, bmD, bmE, 3500)
		runMatch(t, superset, bmA, bmB, bmC, bmD, bmE, bmF, 3551)

		runMatch(t, superset, bigB, bigA, bmF, bmE, bmD, bmC, 5005)
		runMatch(t, bigB, bigA, bmF, bmE, bmD, bmC, bmB, 3511)
		runMatch(t, bigA, bmF, bmE, bmD, bmC, bmB, bmA, 4167)
		runMatch(t, bmF, bmE, bmD, bmC, bmB, bmA, superset, 0)
		runMatch(t, bmE, bmD, bmC, bmB, bmA, superset, bigB, 953)
		runNoMatch(t, bmD, bmC, bmB, bmA, superset, bigB, bigA, 131972)
		runMatch(t, bmC, bmB, bmA, superset, bigB, bigA, bmF, 4007)
		runMatch(t, bmB, bmA, superset, bigB, bigA, bmF, bmE, 2731)
		runNoMatch(t, bmA, superset, bigB, bigA, bmF, bmE, bmD, 25088)
	})

	t.Run("cardinality", func(t *testing.T) {
		and1Card := func(aa, bb *Bitmap) int {
			aa = aa.Clone()
			aa.AndOld(bb)
			return aa.GetCardinality()
		}
		and2Card := func(aa, bb *Bitmap) int {
			return aa.Clone().And(bb).GetCardinality()
		}
		and4Card := func(aa, bb *Bitmap) int {
			return aa.Clone().AndConc(bb, maxConcurrency).GetCardinality()
		}
		and6Card := func(aa, bb *Bitmap) int {
			return AndOld(aa, bb).GetCardinality()
		}
		and7Card := func(aa, bb *Bitmap) int {
			return And(aa, bb).GetCardinality()
		}

		andNot1Card := func(aa, bb *Bitmap) int {
			aa = aa.Clone()
			aa.AndNotOld(bb)
			return aa.GetCardinality()
		}
		andNot2Card := func(aa, bb *Bitmap) int {
			return aa.Clone().AndNot(bb).GetCardinality()
		}
		andNot4Card := func(aa, bb *Bitmap) int {
			return aa.Clone().AndNotConc(bb, maxConcurrency).GetCardinality()
		}
		andNot6Card := func(aa, bb *Bitmap) int {
			return AndNot(aa, bb).GetCardinality()
		}

		or1Card := func(aa, bb *Bitmap) int {
			aa = aa.Clone()
			aa.OrOld(bb)
			return aa.GetCardinality()
		}
		or2Card := func(aa, bb *Bitmap) int {
			return aa.Clone().Or(bb).GetCardinality()
		}
		or4Card := func(aa, bb *Bitmap) int {
			return aa.Clone().OrConc(bb, maxConcurrency).GetCardinality()
		}
		or6Card := func(aa, bb *Bitmap) int {
			return OrOld(aa, bb).GetCardinality()
		}
		or7Card := func(aa, bb *Bitmap) int {
			return Or(aa, bb).GetCardinality()
		}

		run := func(t *testing.T, a, b *Bitmap) {
			and := a.Clone()
			and.AndOld(b)

			aCard := a.GetCardinality()
			bCard := b.GetCardinality()
			andCard := and.GetCardinality()
			andNotACard := aCard - andCard
			orCard := aCard + bCard - andCard

			t.Run("and card", func(t *testing.T) {
				require.Equal(t, andCard, and1Card(a, b))
				require.Equal(t, andCard, and2Card(a, b))
				require.Equal(t, andCard, and4Card(a, b))
				require.Equal(t, andCard, and6Card(a, b))
				require.Equal(t, andCard, and7Card(a, b))
			})

			t.Run("andNot card", func(t *testing.T) {
				require.Equal(t, andNotACard, andNot1Card(a, b))
				require.Equal(t, andNotACard, andNot2Card(a, b))
				require.Equal(t, andNotACard, andNot4Card(a, b))
				require.Equal(t, andNotACard, andNot6Card(a, b))
			})

			t.Run("or card", func(t *testing.T) {
				require.Equal(t, orCard, or1Card(a, b))
				require.Equal(t, orCard, or2Card(a, b))
				require.Equal(t, orCard, or4Card(a, b))
				require.Equal(t, orCard, or6Card(a, b))
				require.Equal(t, orCard, or7Card(a, b))
			})
		}

		bitmaps := []*Bitmap{bmA, bmB, bmC, bmD, bmE, bmF, bigA, bigB, superset}
		for i := range bitmaps {
			for j := range bitmaps {
				run(t, bitmaps[i], bitmaps[j])
			}
		}
	})
}

func TestCompareMergeImplementationsConcurrent(t *testing.T) {
	randSeed := int64(1724861525311)
	rnd := rand.New(rand.NewSource(randSeed))
	maxX := 12345678

	bm1 := NewBitmap()
	bm2 := NewBitmap()
	bm3 := NewBitmap()

	for i := 0; i < 200_000; i++ {
		x := uint64(rnd.Int63n(int64(maxX)))
		switch i % 5 {
		case 0:
			bm1.Set(x)
			bm2.Set(x)
		case 1:
			bm2.Set(x)
			bm3.Set(x)
		case 2:
			bm1.Set(x)
			bm3.Set(x)
		default:
			bm1.Set(x)
			bm2.Set(x)
			bm3.Set(x)
		}
	}

	t.Run("and", func(t *testing.T) {
		bmAnd := bm1.Clone().And(bm2).And(bm3)
		bmAndConc := bm1.Clone().AndConc(bm2, 4).AndConc(bm3, 8)

		assertMatches(t, bmAnd, bmAndConc)
	})

	t.Run("and not", func(t *testing.T) {
		bmAndNot := bm1.Clone().AndNot(bm2).AndNot(bm3)
		bmAndNotConc := bm1.Clone().AndNotConc(bm2, 4).AndNotConc(bm3, 8)

		assertMatches(t, bmAndNot, bmAndNotConc)
	})

	t.Run("or", func(t *testing.T) {
		bmOr := bm1.Clone().Or(bm2).Or(bm3)
		bmOrConc := bm1.Clone().OrConc(bm2, 4).OrConc(bm3, 8)

		assertMatches(t, bmOr, bmOrConc)
	})

	t.Run("mixed", func(t *testing.T) {
		bmMix := bm1.Clone().Or(bm2).And(bm3).AndNot(bm1)
		bmMixConc := bm1.Clone().OrConc(bm2, 4).AndConc(bm3, 8).AndNotConc(bm1, 6)

		assertMatches(t, bmMix, bmMixConc)
	})
}

// checks if all exclusive containers from src bitmap
// are copied to dst bitmap
func TestIssue_Or_NotMergeContainers(t *testing.T) {
	t.Run("fixed values", func(t *testing.T) {
		x0 := uint64(58248)
		x2 := uint64(139024)
		y1 := uint64(123143)
		y2 := uint64(131972)

		bmX := NewBitmap()
		bmX.Set(x0) // container 0
		bmX.Set(x2) // container 2

		bmY := NewBitmap()
		bmY.Set(y1) // container 1
		bmY.Set(y2) // container 2

		require.Equal(t, 2, bmX.GetCardinality())
		require.Equal(t, 2, bmY.GetCardinality())

		// before fix container 2 was copied from bm2 instead
		// being merged with matching container of bm1
		// resulting in one value being lost
		bmX.Or(bmY)

		require.Equal(t, 4, bmX.GetCardinality())
		require.ElementsMatch(t, []uint64{x0, x2, y1, y2}, bmX.ToArray())
	})

	t.Run("generated combinations", func(t *testing.T) {
		// each value belongs to different container
		xs := []uint64{
			1,
			1 + uint64(maxCardinality),
			1 + uint64(maxCardinality)*2,
			1 + uint64(maxCardinality)*3,
			1 + uint64(maxCardinality)*4,
			1 + uint64(maxCardinality)*5,
			1 + uint64(maxCardinality)*6,
			1 + uint64(maxCardinality)*7,
		}

		// values are unique, but belongs to same containers
		// (matching containers should be merged into common ones)
		ys := []uint64{
			1 + uint64(maxCardinality)*8,
			1 + uint64(maxCardinality)*9,
		}
		zs := []uint64{
			2 + uint64(maxCardinality)*8,
			2 + uint64(maxCardinality)*9,
		}
		all := append(append(xs, ys...), zs...)

		assertOr := func(t *testing.T, dst, src *Bitmap) {
			bm := dst.Clone().Or(src)

			require.Equal(t, len(all), bm.GetCardinality())
			require.ElementsMatch(t, all, bm.ToArray())
		}

		// 8 values belonging to 8 different containers are spread
		// between 2 bitmaps in all combinations.
		// 4 values belonging to 2 different containers are added
		// to both bitmaps, so both of them have matching containers
		// that are supposed to be merged (contrary to above containers,
		// that will be entirely copied)

		t.Run("1 of 8", func(t *testing.T) {
			for a := 0; a < len(xs); a++ {
				bmA := NewBitmap()
				bmB := NewBitmap()

				for i := 0; i < len(xs); i++ {
					if i != a {
						bmA.Set(xs[i])
					} else {
						bmB.Set(xs[i])
					}
				}
				for i := 0; i < len(ys); i++ {
					bmA.Set(ys[i])
				}
				for i := 0; i < len(zs); i++ {
					bmB.Set(zs[i])
				}

				require.Equal(t, len(ys)+len(xs)-1, bmA.GetCardinality())
				require.Equal(t, len(zs)+1, bmB.GetCardinality())

				assertOr(t, bmA, bmB)
				assertOr(t, bmB, bmA)
			}
		})

		t.Run("2 of 8", func(t *testing.T) {
			for a := 0; a < len(xs)-1; a++ {
				for b := a + 1; b < len(xs); b++ {
					bmA := NewBitmap()
					bmB := NewBitmap()

					for i := 0; i < len(xs); i++ {
						if i != a && i != b {
							bmA.Set(xs[i])
						} else {
							bmB.Set(xs[i])
						}
					}
					for i := 0; i < len(ys); i++ {
						bmA.Set(ys[i])
					}
					for i := 0; i < len(zs); i++ {
						bmB.Set(zs[i])
					}

					require.Equal(t, len(ys)+len(xs)-2, bmA.GetCardinality())
					require.Equal(t, len(zs)+2, bmB.GetCardinality())

					assertOr(t, bmA, bmB)
					assertOr(t, bmB, bmA)
				}
			}
		})

		t.Run("3 of 8", func(t *testing.T) {
			for a := 0; a < len(xs)-2; a++ {
				for b := a + 1; b < len(xs)-1; b++ {
					for c := b + 1; c < len(xs); c++ {
						bmA := NewBitmap()
						bmB := NewBitmap()

						for i := 0; i < len(xs); i++ {
							if i != a && i != b && i != c {
								bmA.Set(xs[i])
							} else {
								bmB.Set(xs[i])
							}
						}
						for i := 0; i < len(ys); i++ {
							bmA.Set(ys[i])
						}
						for i := 0; i < len(zs); i++ {
							bmB.Set(zs[i])
						}

						require.Equal(t, len(ys)+len(xs)-3, bmA.GetCardinality())
						require.Equal(t, len(zs)+3, bmB.GetCardinality())

						assertOr(t, bmA, bmB)
						assertOr(t, bmB, bmA)
					}
				}
			}
		})

		t.Run("4 of 8", func(t *testing.T) {
			for a := 0; a < len(xs)-3; a++ {
				for b := a + 1; b < len(xs)-2; b++ {
					for c := b + 1; c < len(xs)-1; c++ {
						for d := c + 1; d < len(xs); d++ {
							bmA := NewBitmap()
							bmB := NewBitmap()

							for i := 0; i < len(xs); i++ {
								if i != a && i != b && i != c && i != d {
									bmA.Set(xs[i])
								} else {
									bmB.Set(xs[i])
								}
							}
							for i := 0; i < len(ys); i++ {
								bmA.Set(ys[i])
							}
							for i := 0; i < len(zs); i++ {
								bmB.Set(zs[i])
							}

							require.Equal(t, len(ys)+len(xs)-4, bmA.GetCardinality())
							require.Equal(t, len(zs)+4, bmB.GetCardinality())

							assertOr(t, bmA, bmB)
							assertOr(t, bmB, bmA)
						}
					}
				}
			}
		})
	})
}

func TestNumContainers(t *testing.T) {
	t.Run("nil bitmap", func(t *testing.T) {
		var bm *Bitmap
		require.Equal(t, 0, bm.NumContainers())
	})

	t.Run("empty bitmap", func(t *testing.T) {
		// NewBitmap always pre-allocates the zero-key container, which Cleanup
		// also never removes, so an empty bitmap has 1 container.
		bm := NewBitmap()
		require.Equal(t, 1, bm.NumContainers())
	})

	t.Run("single container", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(1)
		require.Equal(t, 1, bm.NumContainers())
	})

	t.Run("grows with each new container", func(t *testing.T) {
		bm := NewBitmap()
		for i, x := range []uint64{1, 1 + uint64(maxCardinality), 1 + uint64(maxCardinality)*2} {
			bm.Set(x)
			require.Equal(t, i+1, bm.NumContainers())
		}
	})

	t.Run("values in same container don't increase count", func(t *testing.T) {
		bm := NewBitmap()
		for x := uint64(0); x < uint64(maxCardinality); x++ {
			bm.Set(x)
		}
		require.Equal(t, 1, bm.NumContainers())
	})

	t.Run("setting maxCardinality+1 yields 2 containers", func(t *testing.T) {
		// The zero-key container covers [0, maxCardinality) and is always
		// pre-allocated, so adding a value just beyond that boundary must
		// create a second container.
		bm := NewBitmap()
		bm.Set(uint64(maxCardinality) + 1)
		require.Equal(t, 2, bm.NumContainers())
	})
}

func TestLenBytes(t *testing.T) {
	t.Run("non-nil bitmap", func(t *testing.T) {
		bm := NewBitmap()

		for _, x := range []int{1, 1 + maxCardinality, 1 + maxCardinality*2} {
			bm.Set(uint64(x))

			require.Equal(t, len(bm.ToBuffer()), bm.LenInBytes())
		}
	})

	t.Run("empty bitmap", func(t *testing.T) {
		bm := NewBitmap()

		// real length is greater then 0, though ToBuffer() returns empty slice
		require.Less(t, 0, bm.LenInBytes())
	})

	t.Run("nil bitmap", func(t *testing.T) {
		var bm *Bitmap

		require.Equal(t, 0, bm.LenInBytes())
	})
}

func TestIntersects(t *testing.T) {
	t.Run("nil bitmaps return false", func(t *testing.T) {
		var a, b *Bitmap
		require.False(t, a.Intersects(b))
		require.False(t, NewBitmap().Intersects(b))
		require.False(t, a.Intersects(NewBitmap()))
	})

	t.Run("empty bitmaps return false", func(t *testing.T) {
		require.False(t, NewBitmap().Intersects(NewBitmap()))
	})

	t.Run("no overlap returns false", func(t *testing.T) {
		a := NewBitmap()
		a.Set(1)
		a.Set(2)
		b := NewBitmap()
		b.Set(3)
		b.Set(4)
		require.False(t, a.Intersects(b))
	})

	t.Run("single common element returns true", func(t *testing.T) {
		a := NewBitmap()
		a.Set(1)
		a.Set(2)
		b := NewBitmap()
		b.Set(2)
		b.Set(3)
		require.True(t, a.Intersects(b))
	})

	t.Run("matches !Clone().And().IsEmpty()", func(t *testing.T) {
		cases := []struct{ aVals, bVals []uint64 }{
			{[]uint64{1, 2, 3}, []uint64{4, 5, 6}},
			{[]uint64{1, 2, 3}, []uint64{3, 4, 5}},
			{[]uint64{1, 2, 3}, []uint64{1, 2, 3}},
			// keys in different containers
			{[]uint64{1, 1 + uint64(maxCardinality)}, []uint64{2, 1 + uint64(maxCardinality)*2}},
			{[]uint64{1, 1 + uint64(maxCardinality)}, []uint64{1 + uint64(maxCardinality)}},
		}
		for _, tc := range cases {
			a, b := NewBitmap(), NewBitmap()
			for _, v := range tc.aVals {
				a.Set(v)
			}
			for _, v := range tc.bVals {
				b.Set(v)
			}
			expected := !a.Clone().And(b).IsEmpty()
			require.Equal(t, expected, a.Intersects(b))
		}
	})

	t.Run("skewed sizes — a much larger than b", func(t *testing.T) {
		a := NewBitmap()
		for i := uint64(0); i < 2000; i++ {
			a.Set(i << 16)
		}
		b := NewBitmap()
		b.Set(1500 << 16)
		require.True(t, a.Intersects(b))
		b2 := NewBitmap()
		b2.Set(9999 << 16) // not in a
		require.False(t, a.Intersects(b2))
	})
}

func TestIntersectsMasked(t *testing.T) {
	const mask = uint64(0x0000FFFFFFFFFFFF)

	t.Run("empty inputs return false", func(t *testing.T) {
		var a, b *Bitmap
		require.False(t, a.IntersectsMasked(b, mask))
		require.False(t, NewBitmap().IntersectsMasked(b, mask))
		require.False(t, a.IntersectsMasked(NewBitmap(), mask))
		require.False(t, NewBitmap().IntersectsMasked(NewBitmap(), mask))
	})

	t.Run("no overlap returns false", func(t *testing.T) {
		a := NewBitmap()
		a.Set(uint64(1)<<48 | 1)
		b := NewBitmap()
		b.Set(uint64(2)<<48 | 2)
		require.False(t, a.IntersectsMasked(b, mask))
	})

	t.Run("match via mask collapse returns true", func(t *testing.T) {
		a := NewBitmap()
		a.Set(1) // key=0, value=1
		// b has key 0x0001<<48|0, masked to key 0: contains value 1
		b := NewBitmap()
		b.Set(uint64(1)<<48 | 1)
		require.True(t, a.IntersectsMasked(b, mask))
	})

	t.Run("matches !Clone().And(bm.Masked(mask)).IsEmpty()", func(t *testing.T) {
		masks := []uint64{0, 0x0000FFFFFFFFFFFF, math.MaxUint64, 0x00000000FFFF0000}
		a := NewBitmap()
		b := NewBitmap()
		for pos := uint64(0); pos < 4; pos++ {
			for v := uint64(0); v < 50; v++ {
				a.Set(pos<<48 | v)
				b.Set((pos+1)<<48 | v)
			}
		}
		for _, m := range masks {
			expected := !a.Clone().And(b.Masked(m)).IsEmpty()
			require.Equal(t, expected, a.IntersectsMasked(b, m), "mask %#x", m)
		}
	})
}

func TestCapBytes(t *testing.T) {
	t.Run("non-nil bitmap", func(t *testing.T) {
		bm := NewBitmap()

		for _, x := range []int{1, 1 + maxCardinality, 1 + maxCardinality*2} {
			bm.Set(uint64(x))

			// ToBuffer() sets cap to len, real cap is >= than buffer's one
			require.LessOrEqual(t, cap(bm.ToBuffer()), bm.capInBytes())
			require.LessOrEqual(t, bm.LenInBytes(), bm.capInBytes())
		}
	})

	t.Run("empty bitmap", func(t *testing.T) {
		bm := NewBitmap()

		// real cap is greater than 0, though ToBuffer() returns empty slice
		require.Less(t, 0, bm.capInBytes())
		require.LessOrEqual(t, bm.LenInBytes(), bm.capInBytes())
	})

	t.Run("nil bitmap", func(t *testing.T) {
		var bm *Bitmap

		require.Equal(t, 0, bm.capInBytes())
	})
}

func TestCloneToBuf(t *testing.T) {
	assertEqualBitmaps := func(t *testing.T, bm, cloned *Bitmap) {
		require.Equal(t, bm.GetCardinality(), cloned.GetCardinality())
		require.Equal(t, bm.LenInBytes(), cloned.LenInBytes())
		require.ElementsMatch(t, bm.ToArray(), cloned.ToArray())
	}

	t.Run("non-nil bitmap", func(t *testing.T) {
		bmEmpty := NewBitmap()

		bm1 := NewBitmap()
		bm1.Set(1)

		bm2 := NewBitmap()
		bm2.Set(1)
		bm2.Set(1 + uint64(maxCardinality))
		bm2.Set(2 + uint64(maxCardinality))

		bm3 := NewBitmap()
		bm3.Set(1)
		bm3.Set(1 + uint64(maxCardinality))
		bm3.Set(2 + uint64(maxCardinality))
		bm3.Set(1 + uint64(maxCardinality)*2)
		bm3.Set(2 + uint64(maxCardinality)*2)
		bm3.Set(3 + uint64(maxCardinality)*2)

		for name, bm := range map[string]*Bitmap{
			"empty": bmEmpty,
			"bm1":   bm1,
			"bm2":   bm2,
			"bm3":   bm3,
		} {
			t.Run(name, func(t *testing.T) {
				lenInBytes := bm.LenInBytes()
				for name, buf := range map[string][]byte{
					"buf equal len":            make([]byte, lenInBytes),
					"buf greater len":          make([]byte, lenInBytes*3/2),
					"buf equal cap":            make([]byte, 0, lenInBytes),
					"buf greater cap":          make([]byte, 0, lenInBytes*3/2),
					"buf less len greater cap": make([]byte, lenInBytes/2, lenInBytes*3/2),
				} {
					t.Run(name, func(t *testing.T) {
						cloned := bm.CloneToBuf(buf)

						assertEqualBitmaps(t, bm, cloned)
						require.Equal(t, cap(buf), cloned.capInBytes())
					})
				}
			})
		}
	})

	t.Run("nil bitmap, cloned as empty bitmap", func(t *testing.T) {
		var bmNil *Bitmap
		bmEmpty := NewBitmap()

		buf := make([]byte, 0, bmEmpty.LenInBytes()*2)
		cloned := bmNil.CloneToBuf(buf)

		assertEqualBitmaps(t, bmEmpty, cloned)
		require.Equal(t, cap(buf), cloned.capInBytes())
	})

	t.Run("source bitmap is not changed on cloned updates", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(1)
		bmLen := bm.LenInBytes()
		bmCap := bm.capInBytes()

		buf := make([]byte, 0, bm.LenInBytes()*4)
		cloned := bm.CloneToBuf(buf)
		cloned.Set(1 + uint64(maxCardinality))
		cloned.Set(1 + uint64(maxCardinality)*2)

		require.Equal(t, bmLen, bm.LenInBytes())
		require.Equal(t, bmCap, bm.capInBytes())
		require.Equal(t, 1, bm.GetCardinality())
		require.ElementsMatch(t, []uint64{1}, bm.ToArray())

		require.Less(t, bmLen, cloned.LenInBytes())
		require.LessOrEqual(t, bmCap, cloned.capInBytes())
		require.Equal(t, 3, cloned.GetCardinality())
		require.Equal(t, []uint64{1, 1 + uint64(maxCardinality), 1 + uint64(maxCardinality)*2}, cloned.ToArray())
	})

	t.Run("reuse bigger buffer to expand size", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(1)

		// buf big enough for additional containers
		buf := make([]byte, 0, bm.LenInBytes()*4)
		cloned := bm.CloneToBuf(buf)
		clonedLen := cloned.LenInBytes()
		clonedCap := cloned.capInBytes()

		cloned.Set(1 + uint64(maxCardinality))
		cloned.Set(1 + uint64(maxCardinality)*2)

		require.Less(t, clonedLen, cloned.LenInBytes())
		require.Equal(t, clonedCap, cloned.capInBytes())

		// Verify that expansion reuses the pre-allocated buffer without
		// allocating new memory. CloneToBuf allocates at most its one Bitmap
		// struct (zero when it inlines and the struct stays on the stack);
		// the Set that triggers data expansion must not allocate since the
		// buffer has sufficient capacity.
		allocs := testing.AllocsPerRun(10, func() {
			c := bm.CloneToBuf(buf)
			c.Set(1 + uint64(maxCardinality))
		})
		require.LessOrEqual(t, allocs, float64(1),
			"at most the Bitmap struct from CloneToBuf; expansion into pre-allocated buffer must not allocate")
	})

	t.Run("panic on smaller buffer size", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(1)
		buf := make([]byte, 0, bm.LenInBytes()-1)
		require.PanicsWithValue(t,
			fmt.Sprintf("InitCloneToBuf: buf too small: need at least %d bytes, got %d", bm.LenInBytes(), cap(buf)),
			func() { bm.CloneToBuf(buf) })
	})

	t.Run("allow buffer of odd size", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(1)
		bmLen := bm.LenInBytes()

		buf := make([]byte, 0, bmLen+3)
		cloned := bm.CloneToBuf(buf)

		require.Equal(t, bmLen, cloned.LenInBytes())
		require.Equal(t, bmLen+2, cloned.capInBytes())
	})
}

func TestFromBufferUnlimited(t *testing.T) {
	assertEqualBitmaps := func(t *testing.T, bm, fromBuf *Bitmap) {
		require.Equal(t, bm.GetCardinality(), fromBuf.GetCardinality())
		require.Equal(t, bm.LenInBytes(), fromBuf.LenInBytes())
		require.ElementsMatch(t, bm.ToArray(), fromBuf.ToArray())
	}

	t.Run("non-nil bitmap", func(t *testing.T) {
		bmEmpty := NewBitmap()

		bm1 := NewBitmap()
		bm1.Set(1)

		bm2 := NewBitmap()
		bm2.Set(1)
		bm2.Set(1 + uint64(maxCardinality))
		bm2.Set(2 + uint64(maxCardinality))

		bm3 := NewBitmap()
		bm3.Set(1)
		bm3.Set(1 + uint64(maxCardinality))
		bm3.Set(2 + uint64(maxCardinality))
		bm3.Set(1 + uint64(maxCardinality)*2)
		bm3.Set(2 + uint64(maxCardinality)*2)
		bm3.Set(3 + uint64(maxCardinality)*2)

		for name, bm := range map[string]*Bitmap{
			"empty": bmEmpty,
			"bm1":   bm1,
			"bm2":   bm2,
			"bm3":   bm3,
		} {
			t.Run(name, func(t *testing.T) {
				lenInBytes := bm.LenInBytes()
				for name, buf := range map[string][]byte{
					"buf equal cap":   make([]byte, lenInBytes),
					"buf greater cap": make([]byte, lenInBytes, lenInBytes*3/2),
				} {
					t.Run(name, func(t *testing.T) {
						bm.CloneToBuf(buf)
						fromBuf := FromBufferUnlimited(buf)

						assertEqualBitmaps(t, bm, fromBuf)
						require.Equal(t, cap(buf), fromBuf.capInBytes())
					})
				}
			})
		}
	})

	t.Run("small buffer, empty bitmap", func(t *testing.T) {
		bmEmpty := NewBitmap()
		buf := make([]byte, 6)
		fromBuf := FromBufferUnlimited(buf)

		assertEqualBitmaps(t, bmEmpty, fromBuf)
		require.Equal(t, bmEmpty.capInBytes(), fromBuf.capInBytes())
	})

	t.Run("reuse bigger buffer to expand size", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(1)

		// buf big enough for additional containers
		buf := make([]byte, bm.LenInBytes(), bm.LenInBytes()*4)
		bm.CloneToBuf(buf)
		fromBuf := FromBufferUnlimited(buf)
		fromBufLen := fromBuf.LenInBytes()
		fromBufCap := fromBuf.capInBytes()

		fromBuf.Set(1 + uint64(maxCardinality))
		fromBuf.Set(1 + uint64(maxCardinality)*2)

		require.Less(t, fromBufLen, fromBuf.LenInBytes())
		require.Equal(t, fromBufCap, fromBuf.capInBytes())
	})

	t.Run("allow buffer of odd cap", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(1)
		bmLen := bm.LenInBytes()

		buf := make([]byte, bmLen, bmLen+3)
		bm.CloneToBuf(buf)
		fromBuf := FromBufferUnlimited(buf)

		require.Equal(t, bmLen, fromBuf.LenInBytes())
		require.Equal(t, bmLen+2, fromBuf.capInBytes())
	})
}

func TestPrefill(t *testing.T) {
	for _, maxX := range []int{
		0, 1, 123_456,
		maxCardinality / 2,
		maxCardinality - 1, maxCardinality, maxCardinality + 1,
		maxCardinality*3 - 1, maxCardinality * 3, maxCardinality*3 + 1,
	} {
		t.Run(fmt.Sprintf("value %d", maxX), func(t *testing.T) {
			bm := Prefill(uint64(maxX))

			assertPrefilled(t, bm, maxX)
		})
	}
}

func TestFillUp(t *testing.T) {
	t.Run("nil bitmap, noop", func(t *testing.T) {
		maxX := maxCardinality + 1
		var bmNil *Bitmap
		bmNil.FillUp(uint64(maxX))

		require.Nil(t, bmNil)
	})

	t.Run("empty small bitmap, resized", func(t *testing.T) {
		maxX := maxCardinality + 1
		bmSmall := NewBitmap()
		lenBytes := bmSmall.LenInBytes()
		capBytes := bmSmall.capInBytes()

		bmSmall.FillUp(uint64(maxX))
		require.Less(t, lenBytes, bmSmall.LenInBytes())
		require.Less(t, capBytes, bmSmall.capInBytes())

		// + 8 (key) + 2x 4100 container - 64 container
		addLen := 2 * (8 + maxContainerSize*2 - minContainerSize)
		require.Equal(t, lenBytes+addLen, bmSmall.LenInBytes())
		require.Equal(t, capBytes+addLen, bmSmall.capInBytes())

		assertPrefilled(t, bmSmall, maxX)
	})

	t.Run("empty big bitmap, reused", func(t *testing.T) {
		maxX := maxCardinality + 1
		bmBig := NewBitmap()
		bmBig.expandNoLengthChange(3 * maxContainerSize) // big enough to fit 2x fullsize container
		lenBytes := bmBig.LenInBytes()
		capBytes := bmBig.capInBytes()

		bmBig.FillUp(uint64(maxX))
		require.Less(t, lenBytes, bmBig.LenInBytes())
		require.Equal(t, capBytes, bmBig.capInBytes())

		// + 8 (key) + 2x 4100 container - 64 container
		addLen := 2 * (8 + maxContainerSize*2 - minContainerSize)
		require.Equal(t, lenBytes+addLen, bmBig.LenInBytes())

		assertPrefilled(t, bmBig, maxX)
	})

	t.Run("max value already >= than given maxX, noop", func(t *testing.T) {
		maxX := maxCardinality + 1

		t.Run("prefilled", func(t *testing.T) {
			bm := Prefill(uint64(maxX))
			lenBytes := bm.LenInBytes()
			capBytes := bm.capInBytes()

			bm.FillUp(uint64(maxX - 10))
			require.Equal(t, lenBytes, bm.LenInBytes())
			require.Equal(t, capBytes, bm.capInBytes())

			bm.FillUp(uint64(maxX))
			require.Equal(t, lenBytes, bm.LenInBytes())
			require.Equal(t, capBytes, bm.capInBytes())
		})

		t.Run("single element", func(t *testing.T) {
			bm := NewBitmap()
			bm.Set(uint64(maxX))
			lenBytes := bm.LenInBytes()
			capBytes := bm.capInBytes()

			bm.FillUp(uint64(maxX - 10))
			require.Equal(t, lenBytes, bm.LenInBytes())
			require.Equal(t, capBytes, bm.capInBytes())

			bm.FillUp(uint64(maxX))
			require.Equal(t, lenBytes, bm.LenInBytes())
			require.Equal(t, capBytes, bm.capInBytes())
		})
	})

	t.Run("current max value in same container as given maxX", func(t *testing.T) {
		t.Run("prefilled bitmap, no resize", func(t *testing.T) {
			for _, prefillX := range []int{
				1023, 1024, 1025, 1039, 1040, 1041,
			} {
				for _, fillUpX := range []int{
					4095, 4096, 4097, 4111, 4112, 4113, maxCardinality - 2, maxCardinality - 1,
				} {
					t.Run(fmt.Sprintf("filled up 1x %d to %d", prefillX, fillUpX), func(t *testing.T) {
						prefilled := Prefill(uint64(prefillX))
						lenBytes := prefilled.LenInBytes()
						capBytes := prefilled.capInBytes()

						prefilled.FillUp(uint64(fillUpX))
						require.Equal(t, lenBytes, prefilled.LenInBytes())
						require.Equal(t, capBytes, prefilled.capInBytes())

						assertPrefilled(t, prefilled, fillUpX)
					})

					t.Run(fmt.Sprintf("filled up 3x %d to %d", prefillX, fillUpX), func(t *testing.T) {
						prefilled := Prefill(uint64(prefillX))
						lenBytes := prefilled.LenInBytes()
						capBytes := prefilled.capInBytes()

						prefilled.FillUp(uint64(fillUpX) - 20)
						prefilled.FillUp(uint64(fillUpX) - 10)
						prefilled.FillUp(uint64(fillUpX))
						require.Equal(t, lenBytes, prefilled.LenInBytes())
						require.Equal(t, capBytes, prefilled.capInBytes())

						assertPrefilled(t, prefilled, fillUpX)
					})
				}
			}
		})

		t.Run("single elem array, no resize", func(t *testing.T) {
			for _, currentMaxX := range []int{
				1023, 1024, 1025, 1039, 1040, 1041,
			} {
				for _, fillUpX := range []int{
					1055, 1056, 1057, 1082,
				} {
					t.Run(fmt.Sprintf("filled 1x %d to %d", currentMaxX, fillUpX), func(t *testing.T) {
						singleElem := NewBitmap()
						singleElem.Set(uint64(currentMaxX))
						lenBytes := singleElem.LenInBytes()
						capBytes := singleElem.capInBytes()

						singleElem.FillUp(uint64(fillUpX))
						require.Equal(t, lenBytes, singleElem.LenInBytes())
						require.Equal(t, capBytes, singleElem.capInBytes())

						assertFilledUp(t, singleElem, currentMaxX, fillUpX)
					})

					t.Run(fmt.Sprintf("filled 3x %d to %d", currentMaxX, fillUpX), func(t *testing.T) {
						singleElem := NewBitmap()
						singleElem.Set(uint64(currentMaxX))
						lenBytes := singleElem.LenInBytes()
						capBytes := singleElem.capInBytes()

						singleElem.FillUp(uint64(fillUpX) - 10)
						singleElem.FillUp(uint64(fillUpX) - 5)
						singleElem.FillUp(uint64(fillUpX))
						require.Equal(t, lenBytes, singleElem.LenInBytes())
						require.Equal(t, capBytes, singleElem.capInBytes())

						assertFilledUp(t, singleElem, currentMaxX, fillUpX)
					})
				}
			}
		})

		t.Run("single elem array, convert to bitmap", func(t *testing.T) {
			for _, currentMaxX := range []int{
				1023, 1024, 1025, 1039, 1040, 1041,
			} {
				for _, fillUpX := range []int{
					4095, 4096, 4097, maxCardinality - 1,
				} {
					t.Run(fmt.Sprintf("filled 1x %d to %d", currentMaxX, fillUpX), func(t *testing.T) {
						singleElem := NewBitmap()
						singleElem.Set(uint64(currentMaxX))
						singleElem.expandNoLengthChange(maxContainerSize)
						lenBytes := singleElem.LenInBytes()
						capBytes := singleElem.capInBytes()

						singleElem.FillUp(uint64(fillUpX))
						require.Less(t, lenBytes, singleElem.LenInBytes())
						require.Equal(t, capBytes, singleElem.capInBytes())

						// + 4100 container
						addLen := 2 * maxContainerSize
						require.Equal(t, lenBytes+addLen, singleElem.LenInBytes())

						assertFilledUp(t, singleElem, currentMaxX, fillUpX)
					})

					t.Run(fmt.Sprintf("filled 3x %d to %d", currentMaxX, fillUpX), func(t *testing.T) {
						singleElem := NewBitmap()
						singleElem.Set(uint64(currentMaxX))
						singleElem.expandNoLengthChange(maxContainerSize)
						lenBytes := singleElem.LenInBytes()
						capBytes := singleElem.capInBytes()

						singleElem.FillUp(uint64(fillUpX) - 3040)
						singleElem.FillUp(uint64(fillUpX) - 1000)
						singleElem.FillUp(uint64(fillUpX))
						require.Less(t, lenBytes, singleElem.LenInBytes())
						require.Equal(t, capBytes, singleElem.capInBytes())

						// + 4100 container
						addLen := 2 * maxContainerSize
						require.Equal(t, lenBytes+addLen, singleElem.LenInBytes())

						assertFilledUp(t, singleElem, currentMaxX, fillUpX)
					})
				}
			}
		})
	})

	t.Run("current max value in different container than given maxX", func(t *testing.T) {
		unchanged := func(prevVal int) int { return prevVal }
		doubled := func(prevVal int) int { return 2 * prevVal }
		plusKeysAndContainers := func(numKeys, numContainers int) func(int) int {
			return func(prevVal int) int {
				// 8 key + 4100 container
				return prevVal + 2*numKeys*8 + 2*numContainers*maxContainerSize
			}
		}

		t.Run("prefilled bitmap", func(t *testing.T) {
			for _, tc := range []struct {
				prefillX      int
				fillUpX       int
				fnExpAddLen   func(prevLen int) (newLen int)
				fnExpAddCap   func(prevCap int) (newCap int)
				fnExp3xAddLen func(prevLen int) (newLen int)
				fnExp3xAddCap func(prevCap int) (newCap int)
			}{
				{
					prefillX:      maxCardinality - 100,
					fillUpX:       maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   doubled,
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: doubled,
				},
				{
					prefillX:      maxCardinality - 100,
					fillUpX:       maxCardinality + 1022,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   doubled,
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: doubled,
				},
				{
					prefillX:      maxCardinality - 100,
					fillUpX:       maxCardinality + 1023,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   doubled,
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: doubled,
				},
				{
					prefillX:      maxCardinality - 100,
					fillUpX:       maxCardinality + 1024,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   doubled,
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: doubled,
				},
				{
					prefillX:      maxCardinality - 100,
					fillUpX:       5*maxCardinality - 1,
					fnExpAddLen:   plusKeysAndContainers(4, 4),
					fnExpAddCap:   plusKeysAndContainers(4, 4),
					fnExp3xAddLen: plusKeysAndContainers(4, 4),
					fnExp3xAddCap: plusKeysAndContainers(4, 4),
				},
				{
					prefillX:      maxCardinality - 100,
					fillUpX:       5 * maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(5, 5),
					fnExpAddCap:   plusKeysAndContainers(5, 5),
					fnExp3xAddLen: plusKeysAndContainers(4+5, 4+1),
					fnExp3xAddCap: func(prevCap int) int {
						// first 4 containers are added, then cap is doubled
						return doubled(plusKeysAndContainers(4, 4)(prevCap))
					},
				},
				{
					prefillX:      maxCardinality - 100,
					fillUpX:       5*maxCardinality + 1,
					fnExpAddLen:   plusKeysAndContainers(5, 5),
					fnExpAddCap:   plusKeysAndContainers(5, 5),
					fnExp3xAddLen: plusKeysAndContainers(4+5, 4+1),
					fnExp3xAddCap: func(prevCap int) int {
						// first 4 containers were added, then cap was doubled
						return doubled(plusKeysAndContainers(4, 4)(prevCap))
					},
				},

				{
					prefillX:      maxCardinality - 50,
					fillUpX:       maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   doubled,
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: doubled,
				},
				{
					prefillX:      maxCardinality - 50,
					fillUpX:       maxCardinality + 1022,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   doubled,
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: doubled,
				},
				{
					prefillX:      maxCardinality - 50,
					fillUpX:       maxCardinality + 1023,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   doubled,
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: doubled,
				},
				{
					prefillX:      maxCardinality - 50,
					fillUpX:       maxCardinality + 1024,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   doubled,
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: doubled,
				},
				{
					prefillX:      maxCardinality - 50,
					fillUpX:       5*maxCardinality - 1,
					fnExpAddLen:   plusKeysAndContainers(4, 4),
					fnExpAddCap:   plusKeysAndContainers(4, 4),
					fnExp3xAddLen: plusKeysAndContainers(4, 4),
					fnExp3xAddCap: plusKeysAndContainers(4, 4),
				},
				{
					prefillX:      maxCardinality - 50,
					fillUpX:       5 * maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(5, 5),
					fnExpAddCap:   plusKeysAndContainers(5, 5),
					fnExp3xAddLen: plusKeysAndContainers(4+5, 4+1),
					fnExp3xAddCap: func(prevCap int) int {
						// first 4 containers were added, then cap was doubled
						return doubled(plusKeysAndContainers(4, 4)(prevCap))
					},
				},
				{
					prefillX:      maxCardinality - 50,
					fillUpX:       5*maxCardinality + 1,
					fnExpAddLen:   plusKeysAndContainers(5, 5),
					fnExpAddCap:   plusKeysAndContainers(5, 5),
					fnExp3xAddLen: plusKeysAndContainers(4+5, 4+1),
					fnExp3xAddCap: func(prevCap int) int {
						// first 4 containers were added, then cap was doubled
						return doubled(plusKeysAndContainers(4, 4)(prevCap))
					},
				},

				{
					prefillX:      maxCardinality - 1,
					fillUpX:       maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(0, 1),
					fnExpAddCap:   unchanged,
					fnExp3xAddLen: plusKeysAndContainers(0, 1),
					fnExp3xAddCap: unchanged,
				},
				{
					prefillX:      maxCardinality - 1,
					fillUpX:       maxCardinality + 1022,
					fnExpAddLen:   plusKeysAndContainers(0, 1),
					fnExpAddCap:   unchanged,
					fnExp3xAddLen: plusKeysAndContainers(0, 1),
					fnExp3xAddCap: unchanged,
				},
				{
					prefillX:      maxCardinality - 1,
					fillUpX:       maxCardinality + 1023,
					fnExpAddLen:   plusKeysAndContainers(0, 1),
					fnExpAddCap:   unchanged,
					fnExp3xAddLen: plusKeysAndContainers(0, 1),
					fnExp3xAddCap: unchanged,
				},
				{
					prefillX:      maxCardinality - 1,
					fillUpX:       maxCardinality + 1024,
					fnExpAddLen:   plusKeysAndContainers(0, 1),
					fnExpAddCap:   unchanged,
					fnExp3xAddLen: plusKeysAndContainers(0, 1),
					fnExp3xAddCap: unchanged,
				},
				{
					prefillX:      maxCardinality - 1,
					fillUpX:       5*maxCardinality - 1,
					fnExpAddLen:   plusKeysAndContainers(4, 4),
					fnExpAddCap:   plusKeysAndContainers(4, 4),
					fnExp3xAddLen: plusKeysAndContainers(4, 4),
					fnExp3xAddCap: plusKeysAndContainers(4, 4),
				},
				{
					prefillX:      maxCardinality - 1,
					fillUpX:       5 * maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(5, 5),
					fnExpAddCap:   plusKeysAndContainers(5, 5),
					fnExp3xAddLen: plusKeysAndContainers(4+0, 4+1),
					fnExp3xAddCap: func(prevCap int) int {
						// 4 containers were added
						return plusKeysAndContainers(4, 4)(prevCap)
					},
				},
				{
					prefillX:      maxCardinality - 1,
					fillUpX:       5*maxCardinality + 1,
					fnExpAddLen:   plusKeysAndContainers(5, 5),
					fnExpAddCap:   plusKeysAndContainers(5, 5),
					fnExp3xAddLen: plusKeysAndContainers(4+0, 4+1),
					fnExp3xAddCap: func(prevCap int) int {
						// 4 containers were added
						return plusKeysAndContainers(4, 4)(prevCap)
					},
				},
			} {
				t.Run(fmt.Sprintf("filled up 1x %d to %d", tc.prefillX, tc.fillUpX), func(t *testing.T) {
					prefilled := Prefill(uint64(tc.prefillX))
					lenBytes := prefilled.LenInBytes()
					capBytes := prefilled.capInBytes()

					prefilled.FillUp(uint64(tc.fillUpX))
					require.Equal(t, tc.fnExpAddLen(lenBytes), prefilled.LenInBytes())
					require.Equal(t, tc.fnExpAddCap(capBytes), prefilled.capInBytes())

					assertPrefilled(t, prefilled, tc.fillUpX)
				})

				t.Run(fmt.Sprintf("filled up 3x %d to %d", tc.prefillX, tc.fillUpX), func(t *testing.T) {
					prefilled := Prefill(uint64(tc.prefillX))
					lenBytes := prefilled.LenInBytes()
					capBytes := prefilled.capInBytes()

					prefilled.FillUp(uint64(tc.fillUpX) - 20)
					prefilled.FillUp(uint64(tc.fillUpX) - 10)
					prefilled.FillUp(uint64(tc.fillUpX))
					require.Equal(t, tc.fnExp3xAddLen(lenBytes), prefilled.LenInBytes())
					require.Equal(t, tc.fnExp3xAddCap(capBytes), prefilled.capInBytes())

					assertPrefilled(t, prefilled, tc.fillUpX)
				})
			}
		})

		t.Run("single elem array, keep common array", func(t *testing.T) {
			for _, tc := range []struct {
				currentMaxX   int
				fillUpX       int
				fnExpAddLen   func(prevLen int) (newLen int)
				fnExpAddCap   func(prevCap int) (newCap int)
				fnExp3xAddLen func(prevLen int) (newLen int)
				fnExp3xAddCap func(prevCap int) (newCap int)
			}{
				{
					currentMaxX:   maxCardinality - 20,
					fillUpX:       maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 20,
					fillUpX:       maxCardinality + 1022,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 20,
					fillUpX:       maxCardinality + 1023,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 20,
					fillUpX:       maxCardinality + 1024,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 20,
					fillUpX:       3*maxCardinality - 1,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 20,
					fillUpX:       3 * maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(3, 3),
					fnExpAddCap:   plusKeysAndContainers(3, 3),
					fnExp3xAddLen: plusKeysAndContainers(2+3, 2+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(2, 2)(prevCap))
					},
				},
				{
					currentMaxX:   maxCardinality - 20,
					fillUpX:       3*maxCardinality + 1,
					fnExpAddLen:   plusKeysAndContainers(3, 3),
					fnExpAddCap:   plusKeysAndContainers(3, 3),
					fnExp3xAddLen: plusKeysAndContainers(2+3, 2+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(2, 2)(prevCap))
					},
				},

				{
					currentMaxX:   maxCardinality - 10,
					fillUpX:       maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 10,
					fillUpX:       maxCardinality + 1022,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 10,
					fillUpX:       maxCardinality + 1023,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 10,
					fillUpX:       maxCardinality + 1024,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 10,
					fillUpX:       3*maxCardinality - 1,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 10,
					fillUpX:       3 * maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(3, 3),
					fnExpAddCap:   plusKeysAndContainers(3, 3),
					fnExp3xAddLen: plusKeysAndContainers(2+3, 2+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(2, 2)(prevCap))
					},
				},
				{
					currentMaxX:   maxCardinality - 10,
					fillUpX:       3*maxCardinality + 1,
					fnExpAddLen:   plusKeysAndContainers(3, 3),
					fnExpAddCap:   plusKeysAndContainers(3, 3),
					fnExp3xAddLen: plusKeysAndContainers(2+3, 2+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(2, 2)(prevCap))
					},
				},

				{
					currentMaxX:   maxCardinality - 1,
					fillUpX:       maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 1,
					fillUpX:       maxCardinality + 1022,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 1,
					fillUpX:       maxCardinality + 1023,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 1,
					fillUpX:       maxCardinality + 1024,
					fnExpAddLen:   plusKeysAndContainers(1, 1),
					fnExpAddCap:   plusKeysAndContainers(1, 1),
					fnExp3xAddLen: plusKeysAndContainers(1, 1),
					fnExp3xAddCap: plusKeysAndContainers(1, 1),
				},
				{
					currentMaxX:   maxCardinality - 1,
					fillUpX:       3*maxCardinality - 1,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 1,
					fillUpX:       3 * maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(3, 3),
					fnExpAddCap:   plusKeysAndContainers(3, 3),
					fnExp3xAddLen: plusKeysAndContainers(2+3, 2+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(2, 2)(prevCap))
					},
				},
				{
					currentMaxX:   maxCardinality - 1,
					fillUpX:       3*maxCardinality + 1,
					fnExpAddLen:   plusKeysAndContainers(3, 3),
					fnExpAddCap:   plusKeysAndContainers(3, 3),
					fnExp3xAddLen: plusKeysAndContainers(2+3, 2+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(2, 2)(prevCap))
					},
				},
			} {
				t.Run(fmt.Sprintf("filled up 1x %d to %d", tc.currentMaxX, tc.fillUpX), func(t *testing.T) {
					singleElem := NewBitmap()
					singleElem.Set(uint64(tc.currentMaxX))
					lenBytes := singleElem.LenInBytes()
					capBytes := singleElem.capInBytes()

					singleElem.FillUp(uint64(tc.fillUpX))
					require.Equal(t, tc.fnExpAddLen(lenBytes), singleElem.LenInBytes())
					require.Equal(t, tc.fnExpAddCap(capBytes), singleElem.capInBytes())

					assertFilledUp(t, singleElem, tc.currentMaxX, tc.fillUpX)
				})

				t.Run(fmt.Sprintf("filled up 3x %d to %d", tc.currentMaxX, tc.fillUpX), func(t *testing.T) {
					singleElem := NewBitmap()
					singleElem.Set(uint64(tc.currentMaxX))
					lenBytes := singleElem.LenInBytes()
					capBytes := singleElem.capInBytes()

					singleElem.FillUp(uint64(tc.fillUpX) - 20)
					singleElem.FillUp(uint64(tc.fillUpX) - 10)
					singleElem.FillUp(uint64(tc.fillUpX))
					require.Equal(t, tc.fnExp3xAddLen(lenBytes), singleElem.LenInBytes())
					require.Equal(t, tc.fnExp3xAddCap(capBytes), singleElem.capInBytes())

					assertFilledUp(t, singleElem, tc.currentMaxX, tc.fillUpX)
				})
			}
		})

		t.Run("single elem array, convert common to bitmap", func(t *testing.T) {
			for _, tc := range []struct {
				currentMaxX   int
				fillUpX       int
				fnExpAddLen   func(prevLen int) (newLen int)
				fnExpAddCap   func(prevCap int) (newCap int)
				fnExp3xAddLen func(prevLen int) (newLen int)
				fnExp3xAddCap func(prevCap int) (newCap int)
			}{
				{
					currentMaxX:   maxCardinality - 200,
					fillUpX:       maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(0+1, 1+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(0, 1)(prevCap))
					},
				},
				{
					currentMaxX:   maxCardinality - 200,
					fillUpX:       maxCardinality + 1022,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 200,
					fillUpX:       maxCardinality + 1023,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 200,
					fillUpX:       maxCardinality + 1024,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 200,
					fillUpX:       3*maxCardinality - 1,
					fnExpAddLen:   plusKeysAndContainers(3, 3),
					fnExpAddCap:   plusKeysAndContainers(3, 3),
					fnExp3xAddLen: plusKeysAndContainers(3, 3),
					fnExp3xAddCap: plusKeysAndContainers(3, 3),
				},
				{
					currentMaxX:   maxCardinality - 200,
					fillUpX:       3 * maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(4, 4),
					fnExpAddCap:   plusKeysAndContainers(4, 4),
					fnExp3xAddLen: plusKeysAndContainers(3+1, 3+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(3, 3)(prevCap))
					},
				},
				{
					currentMaxX:   maxCardinality - 200,
					fillUpX:       3*maxCardinality + 1,
					fnExpAddLen:   plusKeysAndContainers(4, 4),
					fnExpAddCap:   plusKeysAndContainers(4, 4),
					fnExp3xAddLen: plusKeysAndContainers(3+1, 3+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(3, 3)(prevCap))
					},
				},

				{
					currentMaxX:   maxCardinality - 150,
					fillUpX:       maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(0+1, 1+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(0, 1)(prevCap))
					},
				},
				{
					currentMaxX:   maxCardinality - 150,
					fillUpX:       maxCardinality + 1022,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 150,
					fillUpX:       maxCardinality + 1023,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 150,
					fillUpX:       maxCardinality + 1024,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 150,
					fillUpX:       3*maxCardinality - 1,
					fnExpAddLen:   plusKeysAndContainers(3, 3),
					fnExpAddCap:   plusKeysAndContainers(3, 3),
					fnExp3xAddLen: plusKeysAndContainers(3, 3),
					fnExp3xAddCap: plusKeysAndContainers(3, 3),
				},
				{
					currentMaxX:   maxCardinality - 150,
					fillUpX:       3 * maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(4, 4),
					fnExpAddCap:   plusKeysAndContainers(4, 4),
					fnExp3xAddLen: plusKeysAndContainers(3+1, 3+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(3, 3)(prevCap))
					},
				},
				{
					currentMaxX:   maxCardinality - 150,
					fillUpX:       3*maxCardinality + 1,
					fnExpAddLen:   plusKeysAndContainers(4, 4),
					fnExpAddCap:   plusKeysAndContainers(4, 4),
					fnExp3xAddLen: plusKeysAndContainers(3+1, 3+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(3, 3)(prevCap))
					},
				},

				{
					currentMaxX:   maxCardinality - 100,
					fillUpX:       maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(0+1, 1+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(0, 1)(prevCap))
					},
				},
				{
					currentMaxX:   maxCardinality - 100,
					fillUpX:       maxCardinality + 1022,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 100,
					fillUpX:       maxCardinality + 1023,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 100,
					fillUpX:       maxCardinality + 1024,
					fnExpAddLen:   plusKeysAndContainers(2, 2),
					fnExpAddCap:   plusKeysAndContainers(2, 2),
					fnExp3xAddLen: plusKeysAndContainers(2, 2),
					fnExp3xAddCap: plusKeysAndContainers(2, 2),
				},
				{
					currentMaxX:   maxCardinality - 100,
					fillUpX:       3*maxCardinality - 1,
					fnExpAddLen:   plusKeysAndContainers(3, 3),
					fnExpAddCap:   plusKeysAndContainers(3, 3),
					fnExp3xAddLen: plusKeysAndContainers(3, 3),
					fnExp3xAddCap: plusKeysAndContainers(3, 3),
				},
				{
					currentMaxX:   maxCardinality - 100,
					fillUpX:       3 * maxCardinality,
					fnExpAddLen:   plusKeysAndContainers(4, 4),
					fnExpAddCap:   plusKeysAndContainers(4, 4),
					fnExp3xAddLen: plusKeysAndContainers(3+1, 3+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(3, 3)(prevCap))
					},
				},
				{
					currentMaxX:   maxCardinality - 100,
					fillUpX:       3*maxCardinality + 1,
					fnExpAddLen:   plusKeysAndContainers(4, 4),
					fnExpAddCap:   plusKeysAndContainers(4, 4),
					fnExp3xAddLen: plusKeysAndContainers(3+1, 3+1),
					fnExp3xAddCap: func(prevCap int) int {
						return doubled(plusKeysAndContainers(3, 3)(prevCap))
					},
				},
			} {
				t.Run(fmt.Sprintf("filled up 1x %d to %d", tc.currentMaxX, tc.fillUpX), func(t *testing.T) {
					singleElem := NewBitmap()
					singleElem.Set(uint64(tc.currentMaxX))
					lenBytes := singleElem.LenInBytes()
					capBytes := singleElem.capInBytes()

					singleElem.FillUp(uint64(tc.fillUpX))
					require.Equal(t, tc.fnExpAddLen(lenBytes), singleElem.LenInBytes())
					require.Equal(t, tc.fnExpAddCap(capBytes), singleElem.capInBytes())

					assertFilledUp(t, singleElem, tc.currentMaxX, tc.fillUpX)
				})

				t.Run(fmt.Sprintf("filled up 3x %d to %d", tc.currentMaxX, tc.fillUpX), func(t *testing.T) {
					singleElem := NewBitmap()
					singleElem.Set(uint64(tc.currentMaxX))
					lenBytes := singleElem.LenInBytes()
					capBytes := singleElem.capInBytes()

					singleElem.FillUp(uint64(tc.fillUpX) - 20)
					singleElem.FillUp(uint64(tc.fillUpX) - 10)
					singleElem.FillUp(uint64(tc.fillUpX))
					require.Equal(t, tc.fnExp3xAddLen(lenBytes), singleElem.LenInBytes())
					require.Equal(t, tc.fnExp3xAddCap(capBytes), singleElem.capInBytes())

					assertFilledUp(t, singleElem, tc.currentMaxX, tc.fillUpX)
				})
			}
		})
	})
}

func assertPrefilled(t *testing.T, bm *Bitmap, maxX int) {
	require.Equal(t, maxX+1, bm.GetCardinality())

	arr := bm.ToArray()
	require.Len(t, arr, maxX+1)

	for i, x := range arr {
		require.Equal(t, uint64(i), x)
	}
}

func assertFilledUp(t *testing.T, bm *Bitmap, minX, maxX int) {
	require.Equal(t, maxX-minX+1, bm.GetCardinality())

	arr := bm.ToArray()
	require.Equal(t, maxX-minX+1, len(arr))

	for i, x := range arr {
		require.Equal(t, uint64(i+minX), x)
	}
}

func TestPrefillUtils(t *testing.T) {
	t.Run("calcNoFullContainerAndRemainingXs", func(t *testing.T) {
		maxCard64 := uint64(maxCardinality)

		for _, tc := range []struct {
			maxX            uint64
			expNoContainers int
			expNoRemaining  int
		}{
			{
				maxX:            1,
				expNoContainers: 0,
				expNoRemaining:  2,
			},
			{
				maxX:            maxCard64 - 2,
				expNoContainers: 0,
				expNoRemaining:  maxCardinality - 1,
			},
			{
				maxX:            maxCard64 - 1,
				expNoContainers: 1,
				expNoRemaining:  0,
			},
			{
				maxX:            maxCard64,
				expNoContainers: 1,
				expNoRemaining:  1,
			},
			{
				maxX:            maxCard64 + 1,
				expNoContainers: 1,
				expNoRemaining:  2,
			},
			{
				maxX:            4*maxCard64 - 2,
				expNoContainers: 3,
				expNoRemaining:  maxCardinality - 1,
			},
			{
				maxX:            4*maxCard64 - 1,
				expNoContainers: 4,
				expNoRemaining:  0,
			},
			{
				maxX:            4 * maxCard64,
				expNoContainers: 4,
				expNoRemaining:  1,
			},
			{
				maxX:            4*maxCard64 + 1,
				expNoContainers: 4,
				expNoRemaining:  2,
			},
		} {
			t.Run(fmt.Sprintf("maxX %d", tc.maxX), func(t *testing.T) {
				containers, remaining := calcFullContainersAndRemainingCounts(tc.maxX)
				require.Equal(t, tc.expNoContainers, containers)
				require.Equal(t, tc.expNoRemaining, remaining)
			})
		}
	})

	t.Run("setRange", func(t *testing.T) {
		newContainerBitmap := func() bitmap {
			return bitmap(make([]uint16, maxContainerSize))
		}

		onesBitmap := newContainerBitmap()
		onesBitmap.fillWithOnes()

		assertOnes := func(t *testing.T, b bitmap, minY, maxY int) {
			count := 0
			for _, v := range uint16To64SliceUnsafe(b[startIdx:]) {
				count += bits.OnesCount64(v)
			}
			require.Equal(t, maxY-minY+1, count)

			for i := uint16(minY); i <= uint16(maxY); i++ {
				require.True(t, b.has(i))
			}
		}

		type testCase struct {
			minY, maxY int
		}
		testCases := []testCase{
			{minY: 0, maxY: 0},
			{minY: 1, maxY: 11},
			{minY: 2345, maxY: 4567},
			{minY: 4086, maxY: 4096},
		}
		for _, pair := range [][2]int{
			{16, 48},
			{128, 320},
			{112, 384},
			{192, 336},
		} {
			for i := -2; i <= 2; i++ {
				for j := -2; j <= 2; j++ {
					testCases = append(testCases, testCase{
						minY: pair[0] + i,
						maxY: pair[1] + j,
					})
				}
			}
		}

		for _, tc := range testCases {
			t.Run(fmt.Sprintf("minY %d - maxY %d, without ones bitmap", tc.minY, tc.maxY), func(t *testing.T) {
				b := newContainerBitmap()
				b.setRange(tc.minY, tc.maxY, nil)

				assertOnes(t, b, tc.minY, tc.maxY)
			})
			t.Run(fmt.Sprintf("minY %d - maxY %d, with ones bitmap", tc.minY, tc.maxY), func(t *testing.T) {
				b := newContainerBitmap()
				b.setRange(tc.minY, tc.maxY, onesBitmap)

				assertOnes(t, b, tc.minY, tc.maxY)
			})
		}
	})

	t.Run("fillWithOnes", func(t *testing.T) {
		b := bitmap(make([]uint16, maxContainerSize))
		b.fillWithOnes()

		for _, v := range uint16To64SliceUnsafe(b[startIdx:]) {
			require.Equal(t, 64, bits.OnesCount64(v))
		}
	})
}

// go test -v -fuzz FuzzMergeConcurrently -fuzztime 600s -run ^$ github.com/weaviate/sroar
func FuzzMergeConcurrently(f *testing.F) {
	type testCase struct {
		name           string
		numElements    int
		numSubsets     int
		numMerges      int
		maxConcurrency int
		randSeed       int64
	}

	testCases := []testCase{
		{
			name:           "few elements, few subsets",
			numElements:    15_000,
			numSubsets:     3,
			numMerges:      15,
			maxConcurrency: 1,
			randSeed:       1724861525311406000,
		},
		{
			name:           "more elements, more subsets",
			numElements:    70_000,
			numSubsets:     8,
			numMerges:      12,
			maxConcurrency: 6,
			randSeed:       1724861525311406,
		},
		{
			name:           "many elements, many subsets",
			numElements:    250_000,
			numSubsets:     15,
			numMerges:      10,
			maxConcurrency: 10,
			randSeed:       17248615253114,
		},
	}

	for _, tc := range testCases {
		f.Add(tc.numElements, tc.numSubsets, tc.numMerges, tc.maxConcurrency, tc.randSeed)
	}

	f.Fuzz(runMergeConcurrentlyTest)
}

func TestMergeConcurrentlyWithBuffers_VerifyFuzzCallback(t *testing.T) {
	t.Run("single buffer", func(t *testing.T) {
		runMergeConcurrentlyTest(t, 23_456, 17, 9, 1, 1724861525311)
	})

	t.Run("multiple buffers (concurrent)", func(t *testing.T) {
		runMergeConcurrentlyTest(t, 23_456, 17, 9, 4, 1724861525311)
	})
}

func runMergeConcurrentlyTest(t *testing.T,
	numElements, numSubsets, numMerges, maxConcurrency int, randSeed int64,
) {
	if numElements < 100 || numElements > 500_000 {
		return
	}
	if numSubsets < 1 || numSubsets > 25 {
		return
	}
	if numMerges < 1 || numMerges > 50 {
		return
	}
	if maxConcurrency < 1 || maxConcurrency > 32 {
		return
	}

	maxX := maxCardinality * 10 * minContainersPerRoutine
	rnd := rand.New(rand.NewSource(randSeed))

	subsets := make([]*Bitmap, numSubsets)
	supersetConc := NewBitmap()
	somesetConc := NewBitmap()
	var supersetControl, somesetControl *Bitmap

	t.Run("populate bitmaps", func(t *testing.T) {
		for i := 0; i < numElements; i++ {
			x := uint64(rnd.Intn(maxX))
			supersetConc.Set(x)
			somesetConc.Set(x)
		}

		for i := range subsets {
			subsets[i] = NewBitmap()
			// each next subset bitmap contains fewer elements
			// 1/2 of countElements, 1/3, 1/4, ...
			for j, c := 0, numElements/(i+2); j < c; j++ {
				x := uint64(rnd.Intn(maxX))
				subsets[i].Set(x)
				// ensure superset contains element of subset
				supersetConc.Set(x)
			}
		}

		supersetControl = supersetConc.Clone()
		somesetControl = somesetConc.Clone()
	})

	for i := 0; i < numMerges; i++ {
		t.Run("merge bitmaps", func(t *testing.T) {
			id := rnd.Intn(len(subsets))
			subset := subsets[id]

			switch mergeType := rnd.Intn(3); mergeType {
			case 1:
				t.Run(fmt.Sprintf("AND with %d", id), func(t *testing.T) {
					supersetControl.And(subset)
					supersetConc.AndConc(subset, maxConcurrency)
					assertMatches(t, supersetControl, supersetConc)

					somesetControl.And(subset)
					somesetConc.AndConc(subset, maxConcurrency)
					assertMatches(t, somesetControl, somesetConc)
				})
			case 2:
				t.Run(fmt.Sprintf("AND NOT with %d", id), func(t *testing.T) {
					supersetControl.AndNot(subset)
					supersetConc.AndNotConc(subset, maxConcurrency)
					assertMatches(t, supersetControl, supersetConc)

					somesetControl.AndNot(subset)
					somesetConc.AndNotConc(subset, maxConcurrency)
					assertMatches(t, somesetControl, somesetConc)
				})
			default:
				t.Run(fmt.Sprintf("OR with %d", id), func(t *testing.T) {
					supersetControl.Or(subset)
					supersetConc.OrConc(subset, maxConcurrency)
					assertMatches(t, supersetControl, supersetConc)

					somesetControl.Or(subset)
					somesetConc.OrConc(subset, maxConcurrency)
					assertMatches(t, somesetControl, somesetConc)
				})
			}
		})
	}
}

func assertMatches(t *testing.T, expected *Bitmap, others ...*Bitmap) {
	if len(others) == 0 {
		return
	}

	expCard := expected.GetCardinality()
	for i := range others {
		require.Equalf(t, expCard, others[i].GetCardinality(), "different cardinality for bitmap %d", i)
	}

	// check elements match using iterator as
	// require.ElementsMatch(t, bm1.ToArray(), bm2.ToArray())
	// causes fuzz test to fail frequently
	iterator := expected.NewIterator()
	iterators := make([]*Iterator, len(others))
	for i := range others {
		iterators[i] = others[i].NewIterator()
	}

	for j := 0; ; j++ {
		x := iterator.Next()
		for i := range iterators {
			xi := iterators[i].Next()

			require.Equalf(t, x, xi, "different elements at position %d for bitmap %d", j, i)
		}

		if j > 0 && x == 0 {
			break
		}
	}
}

func TestExpandConditionally(t *testing.T) {
	createBitmapWithKeysAndSpace := func(initialKeys []uint64, numAdditionalKeys, sizeAdditionalContainers int) *Bitmap {
		zeroKey := 1
		if slices.Contains(initialKeys, 0) {
			zeroKey = 0
		}

		numInitialKeys := len(initialKeys)
		bm := initBitmapWithCap(&Bitmap{},
			1+zeroKey+numInitialKeys+numAdditionalKeys,
			minContainerSize,
			(-1+zeroKey+numInitialKeys)*minContainerSize+sizeAdditionalContainers)

		for _, k := range initialKeys {
			bm.Set(k + k*uint64(maxCardinality))
		}

		return bm
	}

	bmk := func(initialKeys ...uint64) *Bitmap {
		bm := NewBitmap()
		for _, k := range initialKeys {
			bm.Set(k + k*uint64(maxCardinality))
		}

		return bm
	}

	keysToBms := map[int][]*Bitmap{
		1: {bmk(1), bmk(3), bmk(5), bmk(6)},
		2: {bmk(1, 3), bmk(1, 5), bmk(1, 6), bmk(3, 5), bmk(3, 6), bmk(5, 6)},
		3: {bmk(1, 3, 5), bmk(1, 3, 6), bmk(1, 5, 6), bmk(3, 5, 6)},
		4: {bmk(1, 3, 5, 6)},
	}

	t.Run("keys and containers fit. bm is not changed", func(t *testing.T) {
		additionalKeys := 4
		bm024 := createBitmapWithKeysAndSpace([]uint64{0, 2, 4},
			additionalKeys, additionalKeys*minContainerSize)

		expIds := bm024.ToArray()
		expCapBytes := bm024.capInBytes()
		expKeysSize := bm024.keys.size()

		t.Run("direct expand conditionally", func(t *testing.T) {
			for k := 0; k <= additionalKeys; k++ {
				t.Run(fmt.Sprintf("keys=%d", k), func(t *testing.T) {
					res := bm024.CloneToBuf(make([]byte, expCapBytes))

					res.expandConditionally(k, k*minContainerSize)

					require.Equal(t, expCapBytes, res.capInBytes())
					require.Equal(t, expKeysSize, res.keys.size())
					require.ElementsMatch(t, expIds, res.ToArray())
				})
			}
		})

		t.Run("integration expand conditionally", func(t *testing.T) {
			for k, bms := range keysToBms {
				t.Run(fmt.Sprintf("keys=%d", k), func(t *testing.T) {
					for _, bm := range bms {
						res := bm024.CloneToBuf(make([]byte, expCapBytes))
						resIds := append(expIds, bm.ToArray()...)

						res.Or(bm)

						require.Equal(t, expCapBytes, res.capInBytes())
						require.Equal(t, expKeysSize, res.keys.size())
						require.ElementsMatch(t, resIds, res.ToArray())
					}
				})
			}
		})
	})

	t.Run("keys do not fit, containers do. containers are moved", func(t *testing.T) {
		additionalKeys := 4
		// key takes 8 uint16s. keys node can not fit all additionalKeys,
		// but slice have required capacity for keys node expansion and containers movement
		bm024 := createBitmapWithKeysAndSpace([]uint64{0, 2, 4},
			0, additionalKeys*minContainerSize+additionalKeys*8)

		expIds := bm024.ToArray()
		expCapBytes := bm024.capInBytes()

		t.Run("direct expand conditionally", func(t *testing.T) {
			for k := 1; k <= additionalKeys; k++ {
				t.Run(fmt.Sprintf("keys=%d", k), func(t *testing.T) {
					res := bm024.CloneToBuf(make([]byte, expCapBytes))
					// keys are extended to fit up to max of [current num of keys] or [num new keys]
					expKeysSize := bm024.keys.size() + max(k, bm024.keys.numKeys())*8

					res.expandConditionally(k, k*minContainerSize)

					require.Equal(t, expCapBytes, res.capInBytes())
					require.Equal(t, expKeysSize, res.keys.size())
					require.ElementsMatch(t, expIds, res.ToArray())
				})
			}
		})

		t.Run("integration expand conditionally", func(t *testing.T) {
			for k, bms := range keysToBms {
				t.Run(fmt.Sprintf("keys=%d", k), func(t *testing.T) {
					// keys are extended to fit up to max of [current num of keys] or [num new keys]
					expKeysSize := bm024.keys.size() + max(k, bm024.keys.numKeys())*8

					for _, bm := range bms {
						res := bm024.CloneToBuf(make([]byte, expCapBytes))
						resIds := append(expIds, bm.ToArray()...)

						res.Or(bm)

						require.Equal(t, expCapBytes, res.capInBytes())
						require.Equal(t, expKeysSize, res.keys.size())
						require.ElementsMatch(t, resIds, res.ToArray())
					}
				})
			}
		})
	})

	t.Run("keys fit, containers do not. bm is extended", func(t *testing.T) {
		additionalKeys := 4
		bm024 := createBitmapWithKeysAndSpace([]uint64{0, 2, 4},
			additionalKeys, 0)

		expIds := bm024.ToArray()
		keysSize := bm024.keys.size()
		capBytes := bm024.capInBytes()

		t.Run("direct expand conditionally", func(t *testing.T) {
			for k := 1; k <= additionalKeys; k++ {
				t.Run(fmt.Sprintf("keys=%d", k), func(t *testing.T) {
					res := bm024.CloneToBuf(make([]byte, capBytes))
					expKeysSize := keysSize + k*8
					expCapBytes := capBytes + max(capBytes, (k*minContainerSize+k*8)*2)

					res.expandConditionally(k, k*minContainerSize)

					require.Equal(t, expCapBytes, res.capInBytes())
					require.Equal(t, expKeysSize, res.keys.size())
					require.ElementsMatch(t, expIds, res.ToArray())
				})
			}
		})

		t.Run("integration expand conditionally", func(t *testing.T) {
			for k, bms := range keysToBms {
				t.Run(fmt.Sprintf("keys=%d", k), func(t *testing.T) {
					// keys are extended to fit up to max of [current num of keys] or [num new keys]
					expKeysSize := keysSize + k*8
					expCapBytes := capBytes + max(capBytes, (k*minContainerSize+k*8)*2)

					for _, bm := range bms {
						res := bm024.CloneToBuf(make([]byte, capBytes))
						resIds := append(expIds, bm.ToArray()...)

						res.Or(bm)

						require.Equal(t, expCapBytes, res.capInBytes())
						require.Equal(t, expKeysSize, res.keys.size())
						require.ElementsMatch(t, resIds, res.ToArray())
					}
				})
			}
		})
	})

	t.Run("keys and containers do not. bm is extended", func(t *testing.T) {
		additionalKeys := 4
		bm024 := createBitmapWithKeysAndSpace([]uint64{0, 2, 4}, 0, 0)

		expIds := bm024.ToArray()
		keysSize := bm024.keys.size()
		capBytes := bm024.capInBytes()

		t.Run("direct expand conditionally", func(t *testing.T) {
			for k := 1; k <= additionalKeys; k++ {
				t.Run(fmt.Sprintf("keys=%d", k), func(t *testing.T) {
					res := bm024.CloneToBuf(make([]byte, capBytes))
					numNewKeys := max(bm024.keys.numKeys(), k)
					expKeysSize := keysSize + numNewKeys*8
					expCapBytes := capBytes + max(capBytes, (k*minContainerSize+numNewKeys*8)*2)

					res.expandConditionally(k, k*minContainerSize)

					require.Equal(t, expCapBytes, res.capInBytes())
					require.Equal(t, expKeysSize, res.keys.size())
					require.ElementsMatch(t, expIds, res.ToArray())
				})
			}
		})

		t.Run("integration expand conditionally", func(t *testing.T) {
			for k, bms := range keysToBms {
				t.Run(fmt.Sprintf("keys=%d", k), func(t *testing.T) {
					// keys are extended to fit up to max of [current num of keys] or [num new keys]
					numNewKeys := max(bm024.keys.numKeys(), k)
					expKeysSize := keysSize + numNewKeys*8
					expCapBytes := capBytes + max(capBytes, (k*minContainerSize+numNewKeys*8)*2)

					for _, bm := range bms {
						res := bm024.CloneToBuf(make([]byte, capBytes))
						resIds := append(expIds, bm.ToArray()...)

						res.Or(bm)

						require.Equal(t, expCapBytes, res.capInBytes())
						require.Equal(t, expKeysSize, res.keys.size())
						require.ElementsMatch(t, resIds, res.ToArray())
					}
				})
			}
		})
	})
}

func TestCalcConcurrency(t *testing.T) {
	t.Run("fewer containers than minimum returns 1", func(t *testing.T) {
		require.Equal(t, 1, calcConcurrency(10, 24, 0))
		require.Equal(t, 1, calcConcurrency(0, 24, 0))
		require.Equal(t, 1, calcConcurrency(23, 24, 0))
	})

	t.Run("exactly minimum containers returns 1", func(t *testing.T) {
		require.Equal(t, 1, calcConcurrency(24, 24, 0))
	})

	t.Run("multiple of minimum returns correct concurrency", func(t *testing.T) {
		require.Equal(t, 2, calcConcurrency(48, 24, 0))
		require.Equal(t, 4, calcConcurrency(96, 24, 0))
		require.Equal(t, 10, calcConcurrency(240, 24, 0))
	})

	t.Run("non-multiple truncates to floor", func(t *testing.T) {
		require.Equal(t, 2, calcConcurrency(50, 24, 0)) // 50/24 = 2
		require.Equal(t, 4, calcConcurrency(99, 24, 0)) // 99/24 = 4
		require.Equal(t, 3, calcConcurrency(72, 24, 0)) // 72/24 = 3 exactly
		require.Equal(t, 3, calcConcurrency(95, 24, 0)) // 95/24 = 3
	})

	t.Run("maxConcurrency=1 forces single goroutine", func(t *testing.T) {
		require.Equal(t, 1, calcConcurrency(240, 24, 1))
		require.Equal(t, 1, calcConcurrency(1000, 24, 1))
	})

	t.Run("maxConcurrency=0 means unlimited", func(t *testing.T) {
		require.Equal(t, 4, calcConcurrency(96, 24, 0))
		require.Equal(t, 10, calcConcurrency(240, 24, 0))
	})

	t.Run("maxConcurrency caps concurrency when lower than calculated", func(t *testing.T) {
		require.Equal(t, 2, calcConcurrency(96, 24, 2))
		require.Equal(t, 3, calcConcurrency(240, 24, 3))
	})

	t.Run("maxConcurrency has no effect when higher than calculated", func(t *testing.T) {
		require.Equal(t, 4, calcConcurrency(96, 24, 6))
		require.Equal(t, 4, calcConcurrency(96, 24, 100))
	})

	t.Run("maxConcurrency equal to calculated returns calculated", func(t *testing.T) {
		require.Equal(t, 4, calcConcurrency(96, 24, 4))
	})
}

func TestMaskedAnd(t *testing.T) {
	t.Run("nil inputs", func(t *testing.T) {
		var a, b *Bitmap
		require.Equal(t, 0, MaskedAnd(a, b, math.MaxUint64).GetCardinality())
		require.Equal(t, 0, MaskedAnd(NewBitmap(), b, math.MaxUint64).GetCardinality())
		require.Equal(t, 0, MaskedAnd(a, NewBitmap(), math.MaxUint64).GetCardinality())
	})

	t.Run("empty inputs", func(t *testing.T) {
		require.Equal(t, 0, MaskedAnd(NewBitmap(), NewBitmap(), math.MaxUint64).GetCardinality())
	})

	t.Run("no overlap produces empty result", func(t *testing.T) {
		a := NewBitmap()
		a.Set(0x00010000)
		a.Set(0x00010001)

		b := NewBitmap()
		b.Set(0x00020000)
		b.Set(0x00020001)

		result := MaskedAnd(a, b, math.MaxUint64)
		require.Equal(t, 0, result.GetCardinality())
	})

	t.Run("matches Masked(And(a,b))", func(t *testing.T) {
		masks := []uint64{0, 0x0000FFFFFFFFFFFF, math.MaxUint64, 0x00000000FFFF0000}

		a := NewBitmap()
		b := NewBitmap()
		for pos := uint64(0); pos < 5; pos++ {
			for v := uint64(0); v < 100; v++ {
				a.Set(pos<<48 | v)
				// b overlaps on even positions only
				if pos%2 == 0 {
					b.Set(pos<<48 | v)
				}
			}
		}

		for _, m := range masks {
			expected := And(a, b).Masked(m)
			got := MaskedAnd(a, b, m)

			require.Equal(t, expected.GetCardinality(), got.GetCardinality(), "mask %#x", m)
			for _, v := range expected.ToArray() {
				require.True(t, got.Contains(v), "mask %#x missing %d", m, v)
			}
		}
	})

	t.Run("key collision after masking merges via OR", func(t *testing.T) {
		// Two key pairs that AND to non-empty, both mapping to same masked key.
		a := NewBitmap()
		b := NewBitmap()

		// pos=1: a has {0,1}, b has {0,1} → AND = {0,1}
		a.Set(0x0001_0000_0000 | 0)
		a.Set(0x0001_0000_0000 | 1)
		b.Set(0x0001_0000_0000 | 0)
		b.Set(0x0001_0000_0000 | 1)

		// pos=2: a has {2,3}, b has {2,3} → AND = {2,3}
		a.Set(0x0002_0000_0000 | 2)
		a.Set(0x0002_0000_0000 | 3)
		b.Set(0x0002_0000_0000 | 2)
		b.Set(0x0002_0000_0000 | 3)

		// mask zeroes bits 32-63 → both key pairs collapse to masked key 0
		result := MaskedAnd(a, b, 0x00000000FFFF0000)

		require.Equal(t, 4, result.GetCardinality())
		require.True(t, result.Contains(0))
		require.True(t, result.Contains(1))
		require.True(t, result.Contains(2))
		require.True(t, result.Contains(3))
	})

	t.Run("does not modify either source", func(t *testing.T) {
		a := NewBitmap()
		a.Set(uint64(1)<<48 | 10)
		a.Set(uint64(2)<<48 | 20)

		b := NewBitmap()
		b.Set(uint64(1)<<48 | 10)
		b.Set(uint64(3)<<48 | 30)

		aCard := a.GetCardinality()
		bCard := b.GetCardinality()
		_ = MaskedAnd(a, b, 0x0000FFFFFFFFFFFF)

		require.Equal(t, aCard, a.GetCardinality())
		require.Equal(t, bCard, b.GetCardinality())
		require.True(t, a.Contains(uint64(1)<<48|10))
		require.True(t, a.Contains(uint64(2)<<48|20))
		require.True(t, b.Contains(uint64(1)<<48|10))
		require.True(t, b.Contains(uint64(3)<<48|30))
	})

	t.Run("low 16 bits of mask are ignored", func(t *testing.T) {
		a := NewBitmap()
		b := NewBitmap()
		a.Set(uint64(1)<<48 | 1)
		b.Set(uint64(1)<<48 | 1)

		r1 := MaskedAnd(a, b, 0x0000FFFFFFFFFFFF)
		r2 := MaskedAnd(a, b, 0x0000FFFFFFFFFFFF|0xFFFF)

		require.Equal(t, r1.GetCardinality(), r2.GetCardinality())
		for _, v := range r1.ToArray() {
			require.True(t, r2.Contains(v))
		}
	})
}

func TestMaskedAndToBuf(t *testing.T) {
	t.Run("nil inputs", func(t *testing.T) {
		var a, b *Bitmap
		result := MaskedAndToBuf(a, b, math.MaxUint64, make([]byte, 4096))
		require.Equal(t, 0, result.GetCardinality())
	})

	t.Run("matches MaskedAnd results", func(t *testing.T) {
		a := NewBitmap()
		b := NewBitmap()
		for pos := uint64(0); pos < 5; pos++ {
			for v := uint64(0); v < 100; v++ {
				a.Set(pos<<48 | v)
				b.Set(pos<<48 | v)
			}
		}

		masks := []uint64{0, 0x0000FFFFFFFFFFFF, math.MaxUint64, 0x00000000FFFF0000}
		for _, m := range masks {
			expected := MaskedAnd(a, b, m)
			got := MaskedAndToBuf(a, b, m, make([]byte, 1<<20))

			require.Equal(t, expected.GetCardinality(), got.GetCardinality(), "mask %#x", m)
			for _, v := range expected.ToArray() {
				require.True(t, got.Contains(v), "mask %#x missing %d", m, v)
			}
		}
	})

	t.Run("no allocation when buffer is large enough", func(t *testing.T) {
		a := NewBitmap()
		b := NewBitmap()
		for pos := uint64(0); pos < 5; pos++ {
			for v := uint64(0); v < 100; v++ {
				a.Set(pos<<48 | v)
				b.Set(pos<<48 | v)
			}
		}

		bufSize := 1 << 20
		result := MaskedAndToBuf(a, b, 0x0000FFFFFFFFFFFF, make([]byte, bufSize))

		require.Greater(t, result.GetCardinality(), 0)
		require.Equal(t, bufSize, result.capInBytes(), "capacity should not change")
	})
}

func TestAndMasked(t *testing.T) {
	t.Run("empty b zeroes ra", func(t *testing.T) {
		ra := NewBitmap()
		ra.Set(1)
		ra.Set(2)
		ra.AndMasked(NewBitmap(), math.MaxUint64)
		require.Equal(t, 0, ra.GetCardinality())
	})

	t.Run("nil b zeroes ra", func(t *testing.T) {
		ra := NewBitmap()
		ra.Set(1)
		var b *Bitmap
		ra.AndMasked(b, math.MaxUint64)
		require.Equal(t, 0, ra.GetCardinality())
	})

	t.Run("identity mask matches ra.And(b)", func(t *testing.T) {
		for _, mask := range []uint64{math.MaxUint64, math.MaxUint64 | 0xFFFF} {
			ra := NewBitmap()
			b := NewBitmap()
			for i := uint64(0); i < 200; i++ {
				ra.Set(i)
				if i%2 == 0 {
					b.Set(i)
				}
			}

			expected := ra.Clone()
			expected.And(b)

			ra.AndMasked(b, mask)

			require.Equal(t, expected.GetCardinality(), ra.GetCardinality(), "mask %#x", mask)
			for _, v := range expected.ToArray() {
				require.True(t, ra.Contains(v), "mask %#x missing %d", mask, v)
			}
		}
	})

	t.Run("matches ra.Clone().And(b.Masked(mask))", func(t *testing.T) {
		masks := []uint64{0, 0x0000FFFFFFFFFFFF, math.MaxUint64, 0x00000000FFFF0000}

		b := NewBitmap()
		for pos := uint64(0); pos < 5; pos++ {
			for v := uint64(0); v < 100; v++ {
				b.Set(pos<<48 | v)
			}
		}

		for _, mask := range masks {
			ra := NewBitmap()
			for pos := uint64(0); pos < 3; pos++ {
				for v := uint64(0); v < 100; v++ {
					ra.Set(pos<<48 | v)
				}
			}

			expected := ra.Clone()
			expected.And(b.Masked(mask))

			ra.AndMasked(b, mask)

			require.Equal(t, expected.GetCardinality(), ra.GetCardinality(), "mask %#x", mask)
			for _, v := range expected.ToArray() {
				require.True(t, ra.Contains(v), "mask %#x missing %d", mask, v)
			}
		}
	})

	t.Run("zero mask collapses all b keys to zero", func(t *testing.T) {
		ra := NewBitmap()
		ra.Set(0x00000000 | 1) // key 0, value 1
		ra.Set(0x00000000 | 2) // key 0, value 2
		ra.Set(0x00010000 | 3) // key 1, value 3 — no b key maps here under zero mask

		b := NewBitmap()
		b.Set(0x00010000 | 1) // key 1 → masked 0, value 1
		b.Set(0x00020000 | 2) // key 2 → masked 0, value 2

		// Masked(b) at key 0 = OR({1}, {2}) = {1, 2}
		// ra[key 0] AND {1, 2} = {1, 2} AND {1, 2} = {1, 2}
		// ra[key 1] has no match → zeroed
		ra.AndMasked(b, 0)

		require.Equal(t, 2, ra.GetCardinality())
		require.True(t, ra.Contains(1))
		require.True(t, ra.Contains(2))
		require.False(t, ra.Contains(0x00010000|3))
	})

	t.Run("b not modified", func(t *testing.T) {
		ra := NewBitmap()
		b := NewBitmap()
		for i := uint64(0); i < 100; i++ {
			ra.Set(i)
			b.Set(uint64(1)<<48 | i)
		}
		bCard := b.GetCardinality()
		bValues := b.ToArray()

		ra.AndMasked(b, 0x0000FFFFFFFFFFFF)

		require.Equal(t, bCard, b.GetCardinality())
		for _, v := range bValues {
			require.True(t, b.Contains(v))
		}
	})

	t.Run("multiple b keys OR before AND", func(t *testing.T) {
		ra := NewBitmap()
		// key 0: values {0, 1, 2, 3}
		for v := uint64(0); v <= 3; v++ {
			ra.Set(v)
		}

		b := NewBitmap()
		// Two b keys both masked to 0: first has {1,2}, second has {3,4}
		// OR = {1,2,3,4}; AND with ra[key 0]={0,1,2,3} = {1,2,3}
		b.Set(0x00010000 | 1)
		b.Set(0x00010000 | 2)
		b.Set(0x00020000 | 3)
		b.Set(0x00020000 | 4)

		ra.AndMasked(b, 0)

		require.Equal(t, 3, ra.GetCardinality())
		require.True(t, ra.Contains(1))
		require.True(t, ra.Contains(2))
		require.True(t, ra.Contains(3))
		require.False(t, ra.Contains(0))
		require.False(t, ra.Contains(4))
	})

	// The next four tests target the buffer-swap logic in the OR accumulation loop.
	// The source container (orBuf) is always exactly-sized after copying group[0],
	// so any non-overlapping element OR'd in will not fit inline — the result
	// lands in fallbackBuf and the two buffers are swapped.

	t.Run("non-overlapping arrays: inline fails due to size, buffers swapped", func(t *testing.T) {
		// group[0] has 50 elements → orBuf.indexSize = 54 (4 header + 50 values).
		// group[1] adds 50 non-overlapping elements → result needs indexSize 104.
		// 54 < 104 → inline fails, result is in fallbackBuf, swap occurs.
		ra := NewBitmap()
		for v := uint64(0); v < 100; v++ {
			ra.Set(v)
		}

		b := NewBitmap()
		for v := uint64(0); v < 50; v++ {
			b.Set(0x00010000 | v) // key 1 → masked 0, values 0-49
		}
		for v := uint64(50); v < 100; v++ {
			b.Set(0x00020000 | v) // key 2 → masked 0, values 50-99
		}

		ra.AndMasked(b, 0)

		require.Equal(t, 100, ra.GetCardinality())
		for v := uint64(0); v < 100; v++ {
			require.True(t, ra.Contains(v))
		}
	})

	t.Run("overlapping arrays: inline succeeds, no swap", func(t *testing.T) {
		// group[0] and group[1] have identical values → OR result cardinality
		// equals group[0] cardinality → fits within orBuf.indexSize → no swap.
		ra := NewBitmap()
		for v := uint64(0); v < 50; v++ {
			ra.Set(v)
		}

		b := NewBitmap()
		for v := uint64(0); v < 50; v++ {
			b.Set(0x00010000 | v) // key 1 → masked 0, values 0-49
		}
		for v := uint64(0); v < 50; v++ {
			b.Set(0x00020000 | v) // key 2 → masked 0, same values 0-49
		}

		ra.AndMasked(b, 0)

		require.Equal(t, 50, ra.GetCardinality())
		for v := uint64(0); v < 50; v++ {
			require.True(t, ra.Contains(v))
		}
	})

	t.Run("large non-overlapping arrays: bitmap conversion triggers swap", func(t *testing.T) {
		// cnum + onum = 1228 + 1228 = 2456 >= 2456 → array-to-bitmap conversion.
		// Source container has indexSize = 1232 < maxContainerSize → inline fails,
		// result (bitmap) lands in fallbackBuf, buffers are swapped.
		const half = 1228 // cnum + onum == 2456 == maxContainerSize/5*3 - startIdx

		ra := NewBitmap()
		for v := uint64(0); v < 2*half; v++ {
			ra.Set(v)
		}

		b := NewBitmap()
		for v := uint64(0); v < half; v++ {
			b.Set(0x00010000 | v) // key 1 → masked 0, values 0-1227
		}
		for v := uint64(half); v < 2*half; v++ {
			b.Set(0x00020000 | v) // key 2 → masked 0, values 1228-2455
		}

		ra.AndMasked(b, 0)

		require.Equal(t, 2*half, ra.GetCardinality())
		for v := uint64(0); v < 2*half; v++ {
			require.True(t, ra.Contains(v))
		}
	})

	t.Run("three containers: swap on first pair, then OR into bitmap succeeds inline", func(t *testing.T) {
		// First OR triggers bitmap conversion and swap (result now in orBuf as bitmap).
		// Second OR is bitmap+array: inline always succeeds, fallbackBuf unused.
		// Verifies that orResult correctly points into orBuf after the swap.
		const half = 1228

		ra := NewBitmap()
		for v := uint64(0); v < 3*half; v++ {
			ra.Set(v)
		}

		b := NewBitmap()
		for v := uint64(0); v < half; v++ {
			b.Set(0x00010000 | v) // key 1 → masked 0
		}
		for v := uint64(half); v < 2*half; v++ {
			b.Set(0x00020000 | v) // key 2 → masked 0, triggers swap with key 1
		}
		for v := uint64(2 * half); v < 3*half; v++ {
			b.Set(0x00030000 | v) // key 3 → masked 0, OR'd into bitmap inline
		}

		ra.AndMasked(b, 0)

		require.Equal(t, 3*half, ra.GetCardinality())
		for v := uint64(0); v < 3*half; v++ {
			require.True(t, ra.Contains(v))
		}
	})
}

func TestAndMaskedConc(t *testing.T) {
	// n is large enough to guarantee at least 2 goroutines:
	// calcConcurrency uses minContainersPerRoutine=24, so n >= 48 gives concurrency >= 2.
	const n = minContainersPerRoutine * 3 // 72 keys

	// mask keeps bits 16-31 and zeroes bits 32-63.
	// b keys at (g<<32 | k<<16) all map to ra key (k<<16) under this mask.
	const mask uint64 = 0x00000000FFFF0000

	// assertMatchesSeq verifies that AndMaskedConc produces the same result as
	// AndMasked at several concurrency levels, including one that forces
	// actual goroutine spawning (maxConc=0 means unlimited).
	assertMatchesSeq := func(t *testing.T, ra, b *Bitmap, m uint64) {
		t.Helper()
		expected := ra.Clone()
		expected.AndMasked(b, m)
		for _, maxConc := range []int{1, 2, 4, 0} {
			got := ra.Clone()
			got.AndMaskedConc(b, m, maxConc)
			require.Equal(t, expected.GetCardinality(), got.GetCardinality(), "maxConc=%d", maxConc)
			for _, v := range expected.ToArray() {
				require.True(t, got.Contains(v), "maxConc=%d missing %d", maxConc, v)
			}
		}
	}

	t.Run("empty b zeroes ra", func(t *testing.T) {
		ra := NewBitmap()
		ra.Set(1)
		ra.AndMaskedConc(NewBitmap(), math.MaxUint64, 0)
		require.Equal(t, 0, ra.GetCardinality())
	})

	t.Run("nil b zeroes ra", func(t *testing.T) {
		ra := NewBitmap()
		ra.Set(1)
		var b *Bitmap
		ra.AndMaskedConc(b, math.MaxUint64, 0)
		require.Equal(t, 0, ra.GetCardinality())
	})

	t.Run("identity mask", func(t *testing.T) {
		// Mirror of TestAndMasked/identity_mask_matches_ra.And(b).
		// n ra keys, b has same keys with even-valued elements only.
		ra := NewBitmap()
		b := NewBitmap()
		for k := uint64(0); k < n; k++ {
			for v := uint64(0); v < 200; v++ {
				ra.Set(k<<16 | v)
				if v%2 == 0 {
					b.Set(k<<16 | v)
				}
			}
		}
		assertMatchesSeq(t, ra, b, math.MaxUint64)
	})

	t.Run("matches AndMasked across masks", func(t *testing.T) {
		// Mirror of TestAndMasked/matches_ra.Clone().And(b.Masked(mask)).
		// n ra keys; b spreads the same keys across 5 high-bit positions.
		ra := NewBitmap()
		b := NewBitmap()
		for k := uint64(0); k < n; k++ {
			for v := uint64(0); v < 100; v++ {
				ra.Set(k<<16 | v)
			}
			for pos := uint64(0); pos < 5; pos++ {
				for v := uint64(0); v < 100; v++ {
					b.Set(pos<<32 | k<<16 | v)
				}
			}
		}
		for _, m := range []uint64{0, 0x0000FFFFFFFFFFFF, math.MaxUint64, mask} {
			assertMatchesSeq(t, ra, b, m)
		}
	})

	t.Run("zero mask: most ra containers zeroed out", func(t *testing.T) {
		// Mirror of TestAndMasked/zero_mask_collapses_all_b_keys_to_zero.
		// Under mask=0 only ra key 0 has a match; all others are zeroed.
		// This exercises the zero-out path across n-1 containers concurrently.
		ra := NewBitmap()
		b := NewBitmap()
		for k := uint64(0); k < n; k++ {
			ra.Set(k<<16 | 1)
			ra.Set(k<<16 | 2)
		}
		b.Set(0x00010000 | 1) // maps to masked key 0
		b.Set(0x00020000 | 2) // maps to masked key 0
		assertMatchesSeq(t, ra, b, 0)
	})

	t.Run("multiple b keys OR before AND", func(t *testing.T) {
		// Mirror of TestAndMasked/multiple_b_keys_OR_before_AND.
		// Each ra key has 2 b keys mapping to it; OR triggers buffer swap
		// (non-overlapping arrays, result doesn't fit in source container).
		ra := NewBitmap()
		b := NewBitmap()
		for k := uint64(0); k < n; k++ {
			for v := uint64(0); v <= 3; v++ {
				ra.Set(k<<16 | v)
			}
			b.Set(uint64(1)<<32 | k<<16 | 1)
			b.Set(uint64(1)<<32 | k<<16 | 2)
			b.Set(uint64(2)<<32 | k<<16 | 3)
			b.Set(uint64(2)<<32 | k<<16 | 4)
		}
		assertMatchesSeq(t, ra, b, mask)
	})

	t.Run("overlapping arrays: inline OR succeeds without swap", func(t *testing.T) {
		// Mirror of TestAndMasked/overlapping_arrays:_inline_succeeds,_no_swap.
		// Two b keys per ra key with identical values — OR result fits in source.
		ra := NewBitmap()
		b := NewBitmap()
		for k := uint64(0); k < n; k++ {
			for v := uint64(0); v < 50; v++ {
				ra.Set(k<<16 | v)
				b.Set(uint64(1)<<32 | k<<16 | v)
				b.Set(uint64(2)<<32 | k<<16 | v)
			}
		}
		assertMatchesSeq(t, ra, b, mask)
	})

	t.Run("large non-overlapping arrays: bitmap conversion triggers swap", func(t *testing.T) {
		// Mirror of TestAndMasked/large_non-overlapping_arrays.
		// Per ra key: two b keys with 1228 non-overlapping values each →
		// cnum+onum=2456 triggers array-to-bitmap conversion and buffer swap.
		const half = 1228
		ra := NewBitmap()
		b := NewBitmap()
		for k := uint64(0); k < n; k++ {
			for v := uint64(0); v < 2*half; v++ {
				ra.Set(k<<16 | v)
			}
			for v := uint64(0); v < half; v++ {
				b.Set(uint64(1)<<32 | k<<16 | v)
			}
			for v := uint64(half); v < 2*half; v++ {
				b.Set(uint64(2)<<32 | k<<16 | v)
			}
		}
		assertMatchesSeq(t, ra, b, mask)
	})

	t.Run("three containers: swap then OR into bitmap", func(t *testing.T) {
		// Mirror of TestAndMasked/three_containers:_swap_on_first_pair.
		// Per ra key: first pair triggers bitmap conversion and swap;
		// third container is OR'd inline into the resulting bitmap.
		const half = 1228
		ra := NewBitmap()
		b := NewBitmap()
		for k := uint64(0); k < n; k++ {
			for v := uint64(0); v < 3*half; v++ {
				ra.Set(k<<16 | v)
			}
			for v := uint64(0); v < half; v++ {
				b.Set(uint64(1)<<32 | k<<16 | v)
			}
			for v := uint64(half); v < 2*half; v++ {
				b.Set(uint64(2)<<32 | k<<16 | v)
			}
			for v := uint64(2 * half); v < 3*half; v++ {
				b.Set(uint64(3)<<32 | k<<16 | v)
			}
		}
		assertMatchesSeq(t, ra, b, mask)
	})

	t.Run("b not modified", func(t *testing.T) {
		ra := NewBitmap()
		b := NewBitmap()
		for k := uint64(0); k < n; k++ {
			for v := uint64(0); v < 50; v++ {
				ra.Set(k<<16 | v)
				b.Set(uint64(1)<<32 | k<<16 | v)
				b.Set(uint64(2)<<32 | k<<16 | v)
			}
		}
		bCard := b.GetCardinality()
		bValues := b.ToArray()

		ra.AndMaskedConc(b, mask, 0)

		require.Equal(t, bCard, b.GetCardinality())
		for _, v := range bValues {
			require.True(t, b.Contains(v))
		}
	})
}

func TestCopresenceByMask(t *testing.T) {
	// pos packs three fields into a uint64 to give readable test data:
	//   bits 63-50: hi  (14 bits) — preserved by maskZeroMid
	//   bits 49-36: mid (14 bits) — zeroed by maskZeroMid
	//   bits 35-0:  lo  (36 bits) — preserved by maskZeroMid
	pos := func(hi, mid, lo uint64) uint64 {
		return (hi << 50) | (mid << 36) | lo
	}
	// maskZeroMid keeps hi and lo, zeroes mid. mask & 0xFFFF == 0xFFFF —
	// satisfies CopresenceByMask's mask-shape requirement.
	const maskZeroMid uint64 = ^(uint64(0x3FFF) << 36)

	bm := func(values ...uint64) *Bitmap {
		b := NewBitmap()
		b.SetMany(values)
		return b
	}

	// safeBufSize implements the documented upper bound for CopresenceByMaskToBuf:
	// sum of input byte sizes. Sized this way, the buf is guaranteed to fit the
	// result without internal growth.
	safeBufSize := func(bms []*Bitmap) int {
		total := 0
		for _, b := range bms {
			total += b.LenInBytes()
		}
		return total
	}

	// runBoth exercises a test case through both call paths and asserts the
	// same result via `check`. CopresenceByMaskToBuf is sized using
	// safeBufSize so it never needs to grow internally.
	runBoth := func(t *testing.T, inputs []*Bitmap, mask uint64, check func(t *testing.T, got *Bitmap)) {
		t.Helper()
		t.Run("plain", func(t *testing.T) {
			check(t, CopresenceByMask(inputs, mask))
		})
		t.Run("ToBuf", func(t *testing.T) {
			check(t, CopresenceByMaskToBuf(inputs, mask, make([]byte, safeBufSize(inputs))))
		})
	}

	tests := []struct {
		name   string
		inputs []*Bitmap
		mask   uint64
		want   []uint64
	}{
		// --- Edge cases ----------------------------------------------------
		{
			name:   "empty slice returns empty",
			inputs: []*Bitmap{},
			mask:   maskZeroMid,
			want:   []uint64{},
		},
		{
			name:   "single input is preserved as-is",
			inputs: []*Bitmap{bm(pos(1, 1, 5), pos(2, 7, 9))},
			mask:   maskZeroMid,
			want:   []uint64{pos(1, 1, 5), pos(2, 7, 9)},
		},
		{
			name:   "all inputs empty returns empty",
			inputs: []*Bitmap{bm(), bm()},
			mask:   maskZeroMid,
			want:   []uint64{},
		},
		{
			name:   "first input empty returns empty",
			inputs: []*Bitmap{bm(), bm(pos(1, 1, 1))},
			mask:   maskZeroMid,
			want:   []uint64{},
		},
		{
			name:   "second input empty returns empty",
			inputs: []*Bitmap{bm(pos(1, 1, 1)), bm()},
			mask:   maskZeroMid,
			want:   []uint64{},
		},
		{
			name: "any empty input among many short-circuits to empty",
			inputs: []*Bitmap{
				bm(pos(1, 1, 5)),
				NewBitmap(),
				bm(pos(1, 2, 5)),
			},
			mask: maskZeroMid,
			want: []uint64{},
		},

		// --- Binary cases --------------------------------------------------
		{
			// a={(1,1,1)}, b={(1,2,1),(1,3,1)}. After masking mid=0 each
			// side maps to {(1,0,1)}. Common = {(1,0,1)} → all originals
			// emit (different mid values are kept distinct in the result).
			name:   "shared masked group: union of contributing mid values",
			inputs: []*Bitmap{bm(pos(1, 1, 1)), bm(pos(1, 2, 1), pos(1, 3, 1))},
			mask:   maskZeroMid,
			want:   []uint64{pos(1, 1, 1), pos(1, 2, 1), pos(1, 3, 1)},
		},
		{
			// a→{(1,0,8)}, b→{(2,0,8)}. Different masked groups — no co-presence.
			name:   "different hi yields disjoint masked groups",
			inputs: []*Bitmap{bm(pos(1, 1, 8)), bm(pos(2, 1, 8))},
			mask:   maskZeroMid,
			want:   []uint64{},
		},
		{
			// Same hi, but lo differs: a→{(1,0,1)}, b→{(1,0,2)}. Common = ∅.
			name:   "same hi different lo: no co-presence on lo",
			inputs: []*Bitmap{bm(pos(1, 1, 1)), bm(pos(1, 2, 2))},
			mask:   maskZeroMid,
			want:   []uint64{},
		},
		{
			// a's lo set = {1,2,3}, b's lo = {2}. Only lo=2 is shared.
			// Result emits both originals whose lo is 2.
			name: "lo intersection: only shared lo values survive",
			inputs: []*Bitmap{
				bm(pos(1, 1, 1), pos(1, 1, 2), pos(1, 1, 3)),
				bm(pos(1, 2, 2)),
			},
			mask: maskZeroMid,
			want: []uint64{pos(1, 1, 2), pos(1, 2, 2)},
		},
		{
			// Two distinct masked groups (hi=1 and hi=2) are each co-present
			// on their respective sides; both contribute fully.
			name: "two distinct shared groups both contribute",
			inputs: []*Bitmap{
				bm(pos(1, 1, 1), pos(2, 1, 1)),
				bm(pos(1, 2, 1), pos(2, 2, 1)),
			},
			mask: maskZeroMid,
			want: []uint64{
				pos(1, 1, 1), pos(1, 2, 1),
				pos(2, 1, 1), pos(2, 2, 1),
			},
		},
		{
			// hi=1 only in a, hi=2 in both. Only hi=2 group contributes.
			name: "one shared group, the other only in a is filtered out",
			inputs: []*Bitmap{
				bm(pos(1, 1, 9), pos(2, 1, 9)),
				bm(pos(2, 2, 9)),
			},
			mask: maskZeroMid,
			want: []uint64{pos(2, 1, 9), pos(2, 2, 9)},
		},
		{
			// a→{(3,0,100)}; b→{(1,0,100),(3,0,100)}. Common = {(3,0,100)}.
			// b's pos(1,5,100) is filtered out — its masked value is unique to b.
			name: "extra container in b filtered out by partial overlap",
			inputs: []*Bitmap{
				bm(pos(3, 3, 100)),
				bm(pos(1, 5, 100), pos(3, 4, 100)),
			},
			mask: maskZeroMid,
			want: []uint64{pos(3, 3, 100), pos(3, 4, 100)},
		},
		{
			// a has 5 lo values at mid=1; b has 2 of them at mid=2.
			// Only the 2 shared lo values survive on both sides — interleaved
			// in the want slice in numerical order (which is also ToArray
			// order): (mid=1, lo=2), (mid=1, lo=4), (mid=2, lo=2), (mid=2, lo=4).
			name: "many lo values: only co-present lo values survive",
			inputs: []*Bitmap{
				bm(pos(1, 1, 1), pos(1, 1, 2), pos(1, 1, 3), pos(1, 1, 4), pos(1, 1, 5)),
				bm(pos(1, 2, 2), pos(1, 2, 4)),
			},
			mask: maskZeroMid,
			want: []uint64{
				pos(1, 1, 2), pos(1, 1, 4),
				pos(1, 2, 2), pos(1, 2, 4),
			},
		},
		{
			// mask = ^0 → every value masks to itself → masked AND ≡ exact
			// set intersection of the input bitmaps.
			name:   "mask=^0 emits intersection of overlapping containers",
			inputs: []*Bitmap{bm(10, 20, 30), bm(20, 30, 40)},
			mask:   ^uint64(0),
			want:   []uint64{20, 30},
		},
		{
			// doc spans into the container key: doc=65537 sets bit 16 in v,
			// which lives in the container key (bits 16-63). pos(1,1,65537)
			// and pos(1,1,1) thus sit in DIFFERENT containers, and under
			// maskZeroMid (which preserves the doc-high portion) they land in
			// DIFFERENT masked groups despite sharing hi and mid.
			name: "doc-high contributes to masked container key",
			inputs: []*Bitmap{
				bm(pos(1, 1, 65537), pos(1, 1, 1)),
				bm(pos(1, 2, 65537)),
			},
			mask: maskZeroMid,
			want: []uint64{pos(1, 1, 65537), pos(1, 2, 65537)},
		},
		{
			// One masked group ((hi=1, mid=0)) but each side has multiple
			// distinct exact keys in it, with partial overlap: K(1,1) is in
			// both, K(1,2) only in a, K(1,3) only in b.
			//   a positions: K(1,1)→{1,2}, K(1,2)→{3,4}.
			//   b positions: K(1,1)→{2,5}, K(1,3)→{1,6}.
			//   A_pos = {1,2,3,4}; B_pos = {1,2,5,6}; common_pos = {1,2}.
			// Per key:
			//   K(1,1) (both): (a OR b) AND common_pos = {1,2,5} AND {1,2} = {1,2}.
			//   K(1,2) (a-only): {3,4} AND {1,2} = ∅ → skip.
			//   K(1,3) (b-only): {1,6} AND {1,2} = {1} → emit.
			name: "multiple containers per group, partial exact-key overlap",
			inputs: []*Bitmap{
				bm(pos(1, 1, 1), pos(1, 1, 2), pos(1, 2, 3), pos(1, 2, 4)),
				bm(pos(1, 1, 2), pos(1, 1, 5), pos(1, 3, 1), pos(1, 3, 6)),
			},
			mask: maskZeroMid,
			want: []uint64{pos(1, 1, 1), pos(1, 1, 2), pos(1, 3, 1)},
		},

		// --- N-ary cases (ported from the old chained tests, now direct
		// CopresenceByMask calls). -----------------------------------------
		{
			name: "three-way: all inputs share single co-present group",
			inputs: []*Bitmap{
				bm(pos(1, 1, 5)),
				bm(pos(1, 2, 5)),
				bm(pos(1, 3, 5)),
			},
			mask: maskZeroMid,
			want: []uint64{pos(1, 1, 5), pos(1, 2, 5), pos(1, 3, 5)},
		},
		{
			name: "three-way: one input misses the group → empty",
			inputs: []*Bitmap{
				bm(pos(1, 1, 5)),
				bm(pos(1, 2, 5)),
				bm(pos(2, 3, 5)),
			},
			mask: maskZeroMid,
			want: []uint64{},
		},
		{
			name: "three-way: two distinct groups, all inputs contribute",
			inputs: []*Bitmap{
				bm(pos(1, 1, 5), pos(2, 1, 5)),
				bm(pos(1, 2, 5), pos(2, 2, 5)),
				bm(pos(1, 3, 5), pos(2, 3, 5)),
			},
			mask: maskZeroMid,
			want: []uint64{
				pos(1, 1, 5), pos(1, 2, 5), pos(1, 3, 5),
				pos(2, 1, 5), pos(2, 2, 5), pos(2, 3, 5),
			},
		},
		{
			// Group (hi=1) is in all 3; group (hi=2) is in inputs 1+2 but
			// missing from input 3 → drops out during the AND fold.
			name: "three-way: only the fully co-present group survives",
			inputs: []*Bitmap{
				bm(pos(1, 1, 5), pos(2, 1, 5)),
				bm(pos(1, 2, 5), pos(2, 2, 5)),
				bm(pos(1, 3, 5)),
			},
			mask: maskZeroMid,
			want: []uint64{pos(1, 1, 5), pos(1, 2, 5), pos(1, 3, 5)},
		},
		{
			// Five-way intersection. Group (hi=1) is in all five → emits;
			// group (hi=2) is only in the first four → drops out at the fifth.
			name: "five-way: only fully co-present group survives",
			inputs: []*Bitmap{
				bm(pos(1, 1, 5), pos(2, 1, 5)),
				bm(pos(1, 2, 5), pos(2, 2, 5)),
				bm(pos(1, 3, 5), pos(2, 3, 5)),
				bm(pos(1, 4, 5), pos(2, 4, 5)),
				bm(pos(1, 5, 5)),
			},
			mask: maskZeroMid,
			want: []uint64{
				pos(1, 1, 5), pos(1, 2, 5), pos(1, 3, 5),
				pos(1, 4, 5), pos(1, 5, 5),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runBoth(t, tt.inputs, tt.mask, func(t *testing.T, got *Bitmap) {
				require.Equal(t, tt.want, got.ToArray())
			})
		})
	}

	t.Run("mixed array and bitmap containers in same group", func(t *testing.T) {
		// Force a bitmap container in input 0 by populating one container
		// with thousands of positions; input 1 uses a small array container
		// for the same masked group. Exercises array/bitmap interop in the
		// OR/AND container ops triggered by the algorithm.
		aVals := make([]uint64, 3000)
		for i := range aVals {
			aVals[i] = pos(1, 1, uint64(i))
		}
		inputs := []*Bitmap{bm(aVals...), bm(pos(1, 2, 100), pos(1, 2, 2000), pos(1, 2, 9999))}

		// A_pos = {0..2999}; B_pos = {100, 2000, 9999}; common_pos = {100, 2000}.
		// 9999 is outside A's range, so it doesn't survive.
		want := []uint64{
			pos(1, 1, 100), pos(1, 1, 2000),
			pos(1, 2, 100), pos(1, 2, 2000),
		}
		runBoth(t, inputs, maskZeroMid, func(t *testing.T, got *Bitmap) {
			require.Equal(t, want, got.ToArray())
		})
	})

	t.Run("wide gap between common masked keys exercises galloping", func(t *testing.T) {
		// inputs[0] has 200 distinct masked groups (hi=1..200); inputs[1]
		// has just the last one. The entry-walk's max-key advance must
		// skip past 199 of inputs[0]'s entries — linear advance would step
		// 199 times, while galloping reaches the target in O(log 199) ≈ 8
		// steps. Correctness is what's checked here; the gallop path is
		// exercised regardless.
		aVals := make([]uint64, 200)
		for i := range aVals {
			hi := uint64(i + 1)
			aVals[i] = pos(hi, 1, hi)
		}
		inputs := []*Bitmap{bm(aVals...), bm(pos(200, 1, 200))}

		want := []uint64{pos(200, 1, 200)}
		runBoth(t, inputs, maskZeroMid, func(t *testing.T, got *Bitmap) {
			require.Equal(t, want, got.ToArray())
		})
	})
}

func TestCopresenceByMaskProperties(t *testing.T) {
	pos := func(hi, mid, lo uint64) uint64 {
		return (hi << 50) | (mid << 36) | lo
	}
	const maskZeroMid uint64 = ^(uint64(0x3FFF) << 36)

	bm := func(values ...uint64) *Bitmap {
		b := NewBitmap()
		b.SetMany(values)
		return b
	}

	t.Run("commutative across argument order (N=2)", func(t *testing.T) {
		a := bm(pos(1, 1, 1), pos(2, 1, 1))
		b := bm(pos(1, 2, 1), pos(2, 2, 1))

		ab := CopresenceByMask([]*Bitmap{a, b}, maskZeroMid).ToArray()
		ba := CopresenceByMask([]*Bitmap{b, a}, maskZeroMid).ToArray()
		require.Equal(t, ab, ba)
	})

	t.Run("permutation-invariant across argument order (N=3)", func(t *testing.T) {
		// For inputs with non-empty n-way co-presence, every permutation of
		// the slice must yield the same value set. The algorithm's cursor
		// initialization is order-sensitive but the result is not.
		a := bm(pos(1, 1, 5), pos(2, 1, 5))
		b := bm(pos(1, 2, 5), pos(3, 2, 5))
		c := bm(pos(1, 3, 5), pos(2, 3, 5))

		perms := [][]*Bitmap{
			{a, b, c}, {a, c, b}, {b, a, c}, {b, c, a}, {c, a, b}, {c, b, a},
		}
		var first []uint64
		for i, p := range perms {
			got := CopresenceByMask(p, maskZeroMid).ToArray()
			if i == 0 {
				first = got
				continue
			}
			require.Equal(t, first, got, "permutation %d differed", i)
		}
	})

	t.Run("idempotent: CopresenceByMask([a, a], m) preserves a", func(t *testing.T) {
		a := bm(pos(1, 1, 1), pos(2, 5, 1), pos(3, 9, 2))

		got := CopresenceByMask([]*Bitmap{a, a}, maskZeroMid).ToArray()
		require.Equal(t, a.ToArray(), got)
	})

	t.Run("matches A.Masked(m).And(B.Masked(m)) on co-presence", func(t *testing.T) {
		// The result's masked image must equal the explicit
		// A.Masked(m).And(B.Masked(m)) reference.
		a := bm(
			pos(1, 1, 1), pos(1, 1, 2), pos(2, 1, 1),
			pos(1, 1, 100), pos(3, 9, 100),
		)
		b := bm(
			pos(1, 2, 1), pos(1, 3, 2), pos(2, 7, 1),
			pos(1, 4, 200), pos(5, 0, 100),
		)

		want := a.Masked(maskZeroMid).And(b.Masked(maskZeroMid))
		got := CopresenceByMask([]*Bitmap{a, b}, maskZeroMid).Masked(maskZeroMid)

		require.Equal(t, want.ToArray(), got.ToArray())
	})

	t.Run("mask=^0 reduces to set intersection", func(t *testing.T) {
		a := bm(10, 20, 30, 40)
		b := bm(20, 30, 50)

		got := CopresenceByMask([]*Bitmap{a, b}, ^uint64(0)).ToArray()
		require.Equal(t, []uint64{20, 30}, got)
	})
}

func TestCopresenceByMaskToBuf(t *testing.T) {
	// TestCopresenceByMask runs every table case through both
	// CopresenceByMask and CopresenceByMaskToBuf, so result-correctness is
	// covered there. The subtests below verify ToBuf-specific behaviour
	// that the unified table can't observe.
	pos := func(hi, mid, lo uint64) uint64 {
		return (hi << 50) | (mid << 36) | lo
	}
	const maskZeroMid uint64 = ^(uint64(0x3FFF) << 36)

	bm := func(values ...uint64) *Bitmap {
		b := NewBitmap()
		b.SetMany(values)
		return b
	}

	t.Run("no allocation when buffer is large enough", func(t *testing.T) {
		// With a generously-sized buffer, the result bitmap's data slice
		// should never need to grow — capInBytes stays at the input buffer's
		// original capacity.
		inputs := []*Bitmap{
			bm(pos(1, 1, 5), pos(2, 1, 5), pos(3, 1, 5)),
			bm(pos(1, 2, 5), pos(2, 2, 5)),
			bm(pos(1, 3, 5)),
		}
		bufBytes := 64 * 1024
		buf := make([]byte, bufBytes)
		got := CopresenceByMaskToBuf(inputs, maskZeroMid, buf)

		require.Greater(t, got.GetCardinality(), 0)
		require.Equal(t, bufBytes, got.capInBytes(),
			"result cap should match the input buffer cap when no growth occurred")
	})

	t.Run("single input is cloned into buf, not aliased", func(t *testing.T) {
		// Single-input short-circuit returns a Clone-backed-by-buf. Verify
		// the result is independent of the source.
		a := bm(pos(1, 1, 5), pos(2, 7, 9))
		got := CopresenceByMaskToBuf([]*Bitmap{a}, maskZeroMid, make([]byte, 4096))
		require.Equal(t, a.ToArray(), got.ToArray())
		got.Set(pos(99, 0, 0))
		require.False(t, a.Contains(pos(99, 0, 0)), "result must be a clone, not aliased")
	})

	t.Run("undersized buf still produces correct result (auto-grows)", func(t *testing.T) {
		// Pass a tiny buffer; the bitmap internally allocates more space.
		// Result correctness is preserved.
		inputs := []*Bitmap{
			bm(pos(1, 1, 1), pos(1, 1, 2)),
			bm(pos(1, 2, 1), pos(1, 2, 2)),
		}
		got := CopresenceByMaskToBuf(inputs, maskZeroMid, make([]byte, 8))
		expected := CopresenceByMask(inputs, maskZeroMid)
		require.Equal(t, expected.ToArray(), got.ToArray())
	})
}

func TestContainerSizeForCard(t *testing.T) {
	// Array containers hold their cardinality and nothing more, floored at
	// minContainerSize and capped at maxArrayContainerSize. 2044 is the
	// largest cardinality that still fits the cap (4 header + 2044 values),
	// and it is what the growth path fills a 2048-uint16 array to.
	testCases := []struct {
		card    int
		expSize uint16
		expType uint16
	}{
		{card: 0, expSize: minContainerSize, expType: typeArray},
		{card: 1, expSize: minContainerSize, expType: typeArray},
		{card: 56, expSize: minContainerSize, expType: typeArray},
		{card: 60, expSize: minContainerSize, expType: typeArray},
		{card: 61, expSize: 68, expType: typeArray},
		{card: 1020, expSize: 1024, expType: typeArray},
		{card: 2043, expSize: maxArrayContainerSize, expType: typeArray},
		{card: 2044, expSize: maxArrayContainerSize, expType: typeArray},
		{card: 2045, expSize: maxContainerSize, expType: typeBitmap},
		{card: maxCardinality, expSize: maxContainerSize, expType: typeBitmap},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("card %d", tc.card), func(t *testing.T) {
			sz, typ := containerSizeForCard(tc.card)
			require.Equal(t, tc.expSize, sz)
			require.Equal(t, tc.expType, typ)
		})
	}
}

func TestFromSortedListSmallContainersAreMinSized(t *testing.T) {
	// The floor gives the pre-created empty key-0 container and a filled
	// small one the same size, and leaves a fresh container room to grow
	// before expandContainer has to move everything behind it.
	bm := FromSortedList([]uint64{1 << 20, 1<<20 + 1})
	require.Equal(t, 2, bm.keys.numKeys())

	zeroCont := bm.getContainer(bm.keys.val(0))
	valsCont := bm.getContainer(bm.keys.val(1))
	require.Equal(t, uint64(0), bm.keys.key(0))
	require.Equal(t, 0, getCardinality(zeroCont))
	require.Equal(t, 2, getCardinality(valsCont))
	require.Equal(t, uint16(minContainerSize), zeroCont[indexSize])
	require.Equal(t, uint16(minContainerSize), valsCont[indexSize])
}

// fromSortedListFamily is one width's entry points plus the fixtures the
// shared drivers need, so both widths are exercised by the same tables.
type fromSortedListFamily[T sortedListVal] struct {
	buildName  string
	initName   string
	build      func([]T) *Bitmap
	buildToBuf func([]T, func(sizeBytes int) []byte) *Bitmap
	initToBuf  func([]T, func(sizeBytes int) (*Bitmap, []byte)) *Bitmap

	sample   []T // small sorted input
	unsorted []T // must panic before get is called
	other    []T // distinct content, proves Init overwrites the struct
	pooled   []T // spans several keys, for the zero-alloc check
}

var family64 = fromSortedListFamily[uint64]{
	buildName:  "FromSortedListToBuf",
	initName:   "InitFromSortedListToBuf",
	build:      FromSortedList,
	buildToBuf: FromSortedListToBuf,
	initToBuf:  InitFromSortedListToBuf,
	sample:     []uint64{1, 2, 3},
	unsorted:   []uint64{2, 1},
	other:      []uint64{99, 100_000},
	pooled:     []uint64{1, 2, 3, 1 << 20, 1 << 40},
}

var family32 = fromSortedListFamily[uint32]{
	buildName:  "FromSortedList32ToBuf",
	initName:   "InitFromSortedList32ToBuf",
	build:      FromSortedList32,
	buildToBuf: FromSortedList32ToBuf,
	initToBuf:  InitFromSortedList32ToBuf,
	sample:     []uint32{1, 2, 3},
	unsorted:   []uint32{2, 1},
	other:      []uint32{99, 100_000},
	pooled:     []uint32{1, 2, 3, 1 << 20, 1 << 28},
}

func sortedSeq(n int, stride uint64) []uint64 {
	vals := make([]uint64, n)
	for i := range vals {
		vals[i] = uint64(i) * stride
	}
	return vals
}

func sortedRandom(rnd *rand.Rand, n int, max int64) []uint64 {
	unique := map[uint64]struct{}{}
	for len(unique) < n {
		unique[uint64(rnd.Int63n(max))] = struct{}{}
	}
	vals := make([]uint64, 0, n)
	for v := range unique {
		vals = append(vals, v)
	}
	slices.Sort(vals)
	return vals
}

// offsetBy shifts vals into a higher key, so a case exercises a container
// the build appends rather than the pre-created one at key 0.
func offsetBy(vals []uint64, base uint64) []uint64 {
	out := make([]uint64, len(vals))
	for i, v := range vals {
		out[i] = base + v
	}
	return out
}

func sortedDup(n int, stride uint64, repeat int) []uint64 {
	vals := make([]uint64, 0, n*repeat)
	for i := 0; i < n; i++ {
		for j := 0; j < repeat; j++ {
			vals = append(vals, uint64(i)*stride)
		}
	}
	return vals
}

// fromSortedListCases are the shapes both widths must handle. Every value
// fits in uint32 so one table drives both runs; width-specific shapes are
// added by the callers.
func fromSortedListCases(rnd *rand.Rand) map[string][]uint64 {
	return map[string][]uint64{
		"empty":                         nil,
		"single value":                  {7},
		"few values in key 0":           {1, 2, 3},
		"container boundaries":          {1, 2, 3, 1 << 20, 1<<20 + 1, 1 << 28},
		"not starting at key 0":         {1 << 20, 1<<20 + 5, 1 << 28},
		"array/bitmap threshold 2044":   sortedSeq(2044, 3),
		"array/bitmap threshold 2045":   sortedSeq(2045, 3),
		"one full container":            sortedSeq(65536, 1),
		"dense across containers":       sortedSeq(1_000_000, 1),
		"sparse arrays":                 sortedSeq(100_000, 1000),
		"one value per container":       sortedSeq(1_000, 1<<16),
		"random":                        sortedRandom(rnd, 50_000, 1<<32),
		"last value of first container": {65534, 65535, 65536},
		"duplicates in array container": {1, 1, 2, 3, 3, 3},
		"duplicates collapsing bitmap":  append(offsetBy(sortedDup(1500, 2, 2), 1<<16), 1<<20, 1<<28),
	}
}

// narrow converts a shared case to 32-bit width.
func narrow(t testing.TB, vals []uint64) []uint32 {
	t.Helper()
	out := make([]uint32, len(vals))
	for i, v := range vals {
		if v > math.MaxUint32 {
			t.Fatalf("shared case value %d does not fit in uint32", v)
		}
		out[i] = uint32(v)
	}
	return out
}

// runFromSortedListCases checks one width's constructors against a bitmap
// built with Set, and requires the same bytes the Accumulator produces for
// that content.
func runFromSortedListCases[T sortedListVal](t *testing.T, f fromSortedListFamily[T], cases map[string][]T) {
	t.Helper()
	for name, vals := range cases {
		t.Run(name, func(t *testing.T) {
			expected := NewBitmap()
			for _, v := range vals {
				expected.Set(uint64(v))
			}

			bm := f.build(vals)
			require.Equal(t, expected.GetCardinality(), bm.GetCardinality())
			require.Equal(t, expected.ToArray(), bm.ToArray())

			// Exact-size constructors share their layout: same content must
			// serialize to the same bytes as the Accumulator build.
			acc := NewAccumulator()
			acc.Or(bm)
			require.Equal(t, acc.Bitmap().ToBuffer(), bm.ToBuffer())

			// The serialized form must survive a deserialize cycle.
			rt := FromBuffer(bm.ToBuffer())
			require.Equal(t, expected.GetCardinality(), rt.GetCardinality())
			require.Equal(t, expected.ToArray(), rt.ToArray())

			// ToBuf must produce the identical bitmap from a dirty,
			// oversized buffer.
			var askedBytes, getCalls int
			bmBuf := f.buildToBuf(vals, func(sizeBytes int) []byte {
				askedBytes, getCalls = sizeBytes, getCalls+1
				buf := make([]byte, sizeBytes+100)
				for i := range buf {
					buf[i] = 0xff
				}
				return buf
			})
			switch {
			case bm.IsEmpty():
				// Empty input still asks for a buffer (the minimal live
				// bitmap); its exact size is an implementation detail.
				require.Positive(t, askedBytes)
			case len(vals) == bm.GetCardinality():
				// The requested size is exact for duplicate-free input.
				require.Equal(t, len(bm.ToBuffer()), askedBytes)
			default:
				// Duplicates make it an upper bound.
				require.GreaterOrEqual(t, askedBytes, len(bm.ToBuffer()))
			}
			require.Equal(t, bm.ToBuffer(), bmBuf.ToBuffer())
			require.Equal(t, 1, getCalls, "get must be called exactly once")

			pooled := &Bitmap{}
			initCalls := 0
			bmInit := f.initToBuf(vals, func(sizeBytes int) (*Bitmap, []byte) {
				initCalls++
				return pooled, make([]byte, sizeBytes)
			})
			require.Equal(t, 1, initCalls, "get must be called exactly once")
			require.Same(t, pooled, bmInit)
			require.Equal(t, bm.ToBuffer(), bmInit.ToBuffer())
		})
	}
}

type toleratedCase[T sortedListVal] struct {
	vals     []T
	expected []uint64
}

func runFromSortedListPanicCases[T sortedListVal](t *testing.T, f fromSortedListFamily[T],
	unsorted map[string][]T, tolerated map[string]toleratedCase[T],
) {
	t.Helper()
	for name, vals := range unsorted {
		t.Run(name, func(t *testing.T) {
			require.Panics(t, func() { f.build(vals) })
			require.Panics(t, func() {
				f.buildToBuf(vals, func(sizeBytes int) []byte { return make([]byte, sizeBytes) })
			})
		})
	}
	for name, tc := range tolerated {
		t.Run(name, func(t *testing.T) {
			var bm *Bitmap
			require.NotPanics(t, func() { bm = f.build(tc.vals) })
			require.Equal(t, tc.expected, bm.ToArray())
		})
	}
}

func runFromSortedListToBufContract[T sortedListVal](t *testing.T, f fromSortedListFamily[T]) {
	t.Helper()

	t.Run("input checked before get is called", func(t *testing.T) {
		called := false
		require.Panics(t, func() {
			f.buildToBuf(f.unsorted, func(sizeBytes int) []byte {
				called = true
				return make([]byte, sizeBytes)
			})
		})
		require.False(t, called)
	})

	t.Run("input checked before init get is called", func(t *testing.T) {
		// The Init form hands over a pooled struct and buffer, so calling get
		// before the check would consume a pool entry the panic then strands.
		called := false
		require.Panics(t, func() {
			f.initToBuf(f.unsorted, func(sizeBytes int) (*Bitmap, []byte) {
				called = true
				return &Bitmap{}, make([]byte, sizeBytes)
			})
		})
		require.False(t, called)
	})

	t.Run("adopts full capacity of length limited buffer", func(t *testing.T) {
		bm := f.buildToBuf(f.sample, func(sizeBytes int) []byte { return make([]byte, 0, sizeBytes) })
		require.Equal(t, f.build(f.sample).ToArray(), bm.ToArray())
	})

	// A nil get is a caller error like any other on this path, so it names
	// the constructor rather than surfacing as a bare nil dereference — and
	// it is reported before the input is walked.
	t.Run("panics on nil get", func(t *testing.T) {
		require.PanicsWithValue(t, f.buildName+": get is nil", func() {
			f.buildToBuf(f.sample, nil)
		})
		require.PanicsWithValue(t, f.initName+": get is nil", func() {
			f.initToBuf(f.sample, nil)
		})
		require.PanicsWithValue(t, f.buildName+": get is nil", func() {
			f.buildToBuf(f.unsorted, nil)
		})
	})

	t.Run("init panics on nil struct from get", func(t *testing.T) {
		require.PanicsWithValue(t, f.initName+": get returned a nil *Bitmap", func() {
			f.initToBuf(f.sample, func(sizeBytes int) (*Bitmap, []byte) {
				return nil, make([]byte, sizeBytes)
			})
		})
	})

	t.Run("init builds into provided struct", func(t *testing.T) {
		reused := f.build(f.other)
		bm := f.initToBuf(f.sample, func(sizeBytes int) (*Bitmap, []byte) {
			return reused, make([]byte, sizeBytes)
		})
		require.Same(t, reused, bm)
		require.Equal(t, f.build(f.sample).ToBuffer(), bm.ToBuffer())
	})

	t.Run("init allocates nothing with pooled struct and buffer", func(t *testing.T) {
		pooled := &Bitmap{}
		buf := make([]byte, 1<<12)
		get := func(sizeBytes int) (*Bitmap, []byte) { return pooled, buf[:sizeBytes] }
		allocs := testing.AllocsPerRun(10, func() { f.initToBuf(f.pooled, get) })
		require.Zero(t, allocs)
	})
}

func TestFromSortedList(t *testing.T) {
	rnd := rand.New(rand.NewSource(1724861525311))
	cases := fromSortedListCases(rnd)
	// Shapes only the 64-bit width can express.
	cases["max uint64 values"] = []uint64{1, math.MaxUint64 - 1, math.MaxUint64}
	cases["keys beyond uint32"] = []uint64{1, 2, 3, 1 << 40, 1<<40 + 1, 1 << 48}
	cases["not starting at a low key"] = []uint64{1 << 20, 1<<20 + 5, 1 << 33}
	cases["random across 40 bits"] = sortedRandom(rnd, 50_000, 1<<40)
	runFromSortedListCases(t, family64, cases)
}

// fromSortedListUnsortedCases must panic; fromSortedListToleratedCases must
// not. Every value fits uint32, so one pair of tables drives both widths.
func fromSortedListUnsortedCases() map[string][]uint64 {
	return map[string][]uint64{
		"unsorted":               {1, 3, 2},
		"unsorted at first pair": {3, 1},
		"unsorted across key":    {1 << 20, 1},
	}
}

func fromSortedListToleratedCases() map[string]toleratedCase[uint64] {
	return map[string]toleratedCase[uint64]{
		"duplicate":          {vals: []uint64{1, 2, 2}, expected: []uint64{1, 2}},
		"duplicate at start": {vals: []uint64{0, 0, 1}, expected: []uint64{0, 1}},
	}
}

func TestFromSortedListPanicsOnUnsortedInput(t *testing.T) {
	runFromSortedListPanicCases(t, family64,
		fromSortedListUnsortedCases(), fromSortedListToleratedCases())
}

func TestFromSortedListToBufContract(t *testing.T) {
	runFromSortedListToBufContract(t, family64)
}

// fromSortedListDuplicateCases are duplicate shapes both widths must handle;
// every value fits in uint32. Shapes already covered by the shared case table
// (duplicates in an array container, and a collapse in a non-zero key) are not
// repeated here.
func fromSortedListDuplicateCases() map[string]toleratedCase[uint64] {
	return map[string]toleratedCase[uint64]{
		"duplicates across container boundary": {
			vals:     []uint64{1 << 16, 1 << 16, 1<<16 + 1},
			expected: []uint64{1 << 16, 1<<16 + 1},
		},
		"all same value": {
			vals:     sortedDup(1, 1, 5000),
			expected: []uint64{0},
		},
		"duplicates in bitmap container": {
			// 3000 distinct values repeated twice: a bitmap container whose
			// cardinality counts distinct only.
			vals:     sortedDup(3000, 2, 2),
			expected: sortedSeq(3000, 2),
		},
		"key 0 collapses then more keys follow": {
			// Key 0's segment: 1500 distinct values duplicated to 3000 raw.
			// 3000 sizes the pre-created container as a bitmap, 1500 is under
			// the array threshold, so it is rewritten in place as an array —
			// containers for higher keys must then append correctly after the
			// truncated tail.
			vals:     append(sortedDup(1500, 2, 2), 1<<16, 1<<20, 1<<28),
			expected: append(sortedSeq(1500, 2), 1<<16, 1<<20, 1<<28),
		},
		"duplicates straddling bitmap threshold": {
			// 1500 distinct repeated twice: 3000 raw elements, but the
			// container type follows the 1500 distinct — an array, same as a
			// duplicate-free build.
			vals:     sortedDup(1500, 2, 2),
			expected: sortedSeq(1500, 2),
		},
	}
}

// runFromSortedListDuplicateCases checks that duplicates are collapsed and
// that the resulting layout is the one a duplicate-free build of the same
// distinct values would produce.
func runFromSortedListDuplicateCases[T sortedListVal](t *testing.T, f fromSortedListFamily[T],
	cases map[string]toleratedCase[T],
) {
	t.Helper()
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			bm := f.build(tc.vals)
			require.Equal(t, len(tc.expected), bm.GetCardinality())
			require.Equal(t, tc.expected, bm.ToArray())

			// Sizing by distinct count makes the layout independent of
			// duplicate multiplicity and identical across the exact-size
			// constructors.
			require.Equal(t, FromSortedList(tc.expected).ToBuffer(), bm.ToBuffer())
			acc := NewAccumulator()
			acc.Or(bm)
			require.Equal(t, acc.Bitmap().ToBuffer(), bm.ToBuffer())

			// The fix-up must work identically inside an adopted buffer,
			// where the requested size is an upper bound for duplicate
			// input.
			var askedBytes int
			bmBuf := f.buildToBuf(tc.vals, func(sizeBytes int) []byte {
				askedBytes = sizeBytes
				buf := make([]byte, sizeBytes)
				for i := range buf {
					buf[i] = 0xff
				}
				return buf
			})
			require.GreaterOrEqual(t, askedBytes, len(bm.ToBuffer()))
			require.Equal(t, bm.ToBuffer(), bmBuf.ToBuffer())
		})
	}
}

// TestFromSortedListContainerTypeAtThreshold pins the array/bitmap threshold
// absolutely. Nothing else can: the Set oracle is container-type agnostic,
// and both the Accumulator comparison and the requested-size check route
// through the same containerSizeForCard, so they move with it when it
// changes. Without this, shifting the cutoff by one cardinality passes the
// suite.
func TestFromSortedListContainerTypeAtThreshold(t *testing.T) {
	for _, tc := range []struct {
		name     string
		distinct int
		wantType uint16
	}{
		{"below the threshold", 2043, typeArray},
		{"at the threshold", 2044, typeArray},
		{"past the threshold", 2045, typeBitmap},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vals := sortedSeq(tc.distinct, 1) // one key, so one container
			for width, bm := range map[string]*Bitmap{
				"64": FromSortedList(vals),
				"32": FromSortedList32(narrow(t, vals)),
			} {
				c := bm.getContainer(bm.keys.val(0))
				require.Equal(t, tc.wantType, c[indexType], width)
				require.Equal(t, tc.distinct, bm.GetCardinality(), width)
			}
		})
	}
}

func TestFromSortedListDuplicates(t *testing.T) {
	cases := fromSortedListDuplicateCases()
	// Only the 64-bit width can carry a key past 2^32.
	cases["collapse then a key beyond uint32"] = toleratedCase[uint64]{
		vals:     append(sortedDup(1500, 2, 2), 1<<40),
		expected: append(sortedSeq(1500, 2), 1<<40),
	}
	runFromSortedListDuplicateCases(t, family64, cases)
}

func TestFromSortedList32Duplicates(t *testing.T) {
	cases := map[string]toleratedCase[uint32]{}
	for name, tc := range fromSortedListDuplicateCases() {
		cases[name] = toleratedCase[uint32]{vals: narrow(t, tc.vals), expected: tc.expected}
	}
	runFromSortedListDuplicateCases(t, family32, cases)
}

// widen converts 32-bit values to the []uint64 the 64-bit constructors take.
func widen(vals []uint32) []uint64 {
	wide := make([]uint64, len(vals))
	for i, v := range vals {
		wide[i] = uint64(v)
	}
	return wide
}

func TestFromSortedList32(t *testing.T) {
	cases := map[string][]uint32{}
	for name, vals := range fromSortedListCases(rand.New(rand.NewSource(1724861525311))) {
		cases[name] = narrow(t, vals)
	}
	cases["max uint32 values"] = []uint32{1, math.MaxUint32 - 1, math.MaxUint32}
	runFromSortedListCases(t, family32, cases)

	// The two widths must agree byte for byte, not merely in content.
	for name, vals := range cases {
		t.Run("matches 64-bit/"+name, func(t *testing.T) {
			require.Equal(t, FromSortedList(widen(vals)).ToBuffer(), FromSortedList32(vals).ToBuffer())
		})
	}
}

func TestFromSortedList32PanicsOnUnsortedInput(t *testing.T) {
	unsorted := map[string][]uint32{}
	for name, vals := range fromSortedListUnsortedCases() {
		unsorted[name] = narrow(t, vals)
	}
	tolerated := map[string]toleratedCase[uint32]{}
	for name, tc := range fromSortedListToleratedCases() {
		tolerated[name] = toleratedCase[uint32]{vals: narrow(t, tc.vals), expected: tc.expected}
	}
	runFromSortedListPanicCases(t, family32, unsorted, tolerated)
}

// Every entry point of the family passes its own name down, so a panic
// names the function the caller invoked, not the shared body it runs in.
func TestFromSortedListPanicNamesCaller(t *testing.T) {
	unsorted64, unsorted32 := []uint64{1, 3, 2}, []uint32{1, 3, 2}
	getBuf := func(sizeBytes int) []byte { return make([]byte, sizeBytes) }
	getBoth := func(sizeBytes int) (*Bitmap, []byte) { return &Bitmap{}, make([]byte, sizeBytes) }

	unsortedCallers := map[string]func(){
		"FromSortedList":            func() { FromSortedList(unsorted64) },
		"FromSortedList32":          func() { FromSortedList32(unsorted32) },
		"FromSortedListToBuf":       func() { FromSortedListToBuf(unsorted64, getBuf) },
		"FromSortedList32ToBuf":     func() { FromSortedList32ToBuf(unsorted32, getBuf) },
		"InitFromSortedListToBuf":   func() { InitFromSortedListToBuf(unsorted64, getBoth) },
		"InitFromSortedList32ToBuf": func() { InitFromSortedList32ToBuf(unsorted32, getBoth) },
	}
	for name, call := range unsortedCallers {
		t.Run("unsorted/"+name, func(t *testing.T) {
			require.PanicsWithValue(t, name+": input not sorted at index 2 (2 after 3)", call)
		})
	}

	// The buffer panics come from the shared initBitmapToBufExact, which must
	// name the caller too.
	tooSmall := func(sizeBytes int) []byte { return make([]byte, sizeBytes-2) }
	tooSmallBoth := func(sizeBytes int) (*Bitmap, []byte) { return &Bitmap{}, make([]byte, sizeBytes-2) }
	bufCallers := map[string]func(){
		"FromSortedListToBuf":       func() { FromSortedListToBuf([]uint64{1, 2, 3}, tooSmall) },
		"FromSortedList32ToBuf":     func() { FromSortedList32ToBuf([]uint32{1, 2, 3}, tooSmall) },
		"InitFromSortedListToBuf":   func() { InitFromSortedListToBuf([]uint64{1, 2, 3}, tooSmallBoth) },
		"InitFromSortedList32ToBuf": func() { InitFromSortedList32ToBuf([]uint32{1, 2, 3}, tooSmallBoth) },
	}
	for name, call := range bufCallers {
		t.Run("buffer/"+name, func(t *testing.T) {
			requirePanicPrefix(t, name+": ", call)
		})
	}
}

func requirePanicPrefix(t *testing.T, prefix string, call func()) {
	t.Helper()
	defer func() {
		r := recover()
		require.NotNil(t, r, "expected a panic")
		msg, ok := r.(string)
		require.True(t, ok, "panic value %v is not a string", r)
		require.True(t, strings.HasPrefix(msg, prefix), "panic %q does not start with %q", msg, prefix)
	}()
	call()
}

func TestFromSortedList32ToBufContract(t *testing.T) {
	runFromSortedListToBufContract(t, family32)
}

func TestFromSortedListToBufMutation(t *testing.T) {
	vals := []uint64{1, 2, 3, 1 << 20}

	var pool []byte
	bm := InitFromSortedListToBuf(vals, func(sizeBytes int) (*Bitmap, []byte) {
		pool = make([]byte, sizeBytes, sizeBytes+1024)
		return &Bitmap{}, pool
	})
	oracle := NewBitmap()
	for _, v := range vals {
		oracle.Set(v)
	}
	require.NotNil(t, bm._ptr)

	// Mutations that fit the buffer's spare capacity stay on it.
	mutate := func(b *Bitmap) {
		b.Set(4)
		b.Set(1 << 21)
		b.Remove(2)
	}
	mutate(bm)
	mutate(oracle)
	require.NotNil(t, bm._ptr)
	require.Equal(t, oracle.ToArray(), bm.ToArray())

	// Outgrowing the buffer migrates the bitmap to the heap.
	for v := uint64(0); v < 100_000; v++ {
		bm.Set(v)
		oracle.Set(v)
	}
	require.Nil(t, bm._ptr)

	// The buffer is then free for reuse: trashing it must not affect the
	// bitmap.
	for i := range pool {
		pool[i] = 0xee
	}
	require.Equal(t, oracle.GetCardinality(), bm.GetCardinality())
	require.Equal(t, oracle.ToArray(), bm.ToArray())
}

func TestFromSortedListToBufGetMutatesVals(t *testing.T) {
	// Unchecked, these make the build revisit a key it already finalized,
	// silently losing the values written the first time.
	rejected := map[string]struct {
		vals   []uint64
		mutate func(vals []uint64)
		panics string
	}{
		"value moved to a later key": {
			vals:   []uint64{0, 1, 2},
			mutate: func(vals []uint64) { vals[1] = 1 << 16 },
			panics: "FromSortedListToBuf: vals mutated during build (index 2: 2 after 65536)",
		},
		"values swapped within a key": {
			vals:   []uint64{0, 1, 2},
			mutate: func(vals []uint64) { vals[1], vals[2] = vals[2], vals[1] },
			panics: "FromSortedListToBuf: vals mutated during build (index 2: 1 after 2)",
		},
		"value moved to an earlier key": {
			vals:   []uint64{0, 1 << 16, 2 << 16},
			mutate: func(vals []uint64) { vals[2] = 1 },
			panics: "FromSortedListToBuf: vals mutated during build (index 2: 1 after 65536)",
		},
	}
	for name, tc := range rejected {
		t.Run(name, func(t *testing.T) {
			require.PanicsWithValue(t, tc.panics, func() {
				FromSortedListToBuf(tc.vals, func(sizeBytes int) []byte {
					tc.mutate(tc.vals)
					return make([]byte, sizeBytes)
				})
			})
		})
	}

	// Still ascending, so not rejected: the values are always right. Changing
	// the key set leaves the keys node sized as the layout pass predicted,
	// costing byte-canonicality — documented, not enforced.
	tolerated := map[string]struct {
		vals      []uint64
		mutate    func(vals []uint64)
		expect    []uint64
		canonical bool
	}{
		"value replaced within the same key": {
			vals:      []uint64{0, 1, 2},
			mutate:    func(vals []uint64) { vals[2] = 1000 },
			expect:    []uint64{0, 1, 1000},
			canonical: true,
		},
		"value moved into a new key": {
			vals:   []uint64{0, 1, 2},
			mutate: func(vals []uint64) { vals[2] = 1 << 16 },
			expect: []uint64{0, 1, 1 << 16},
		},
		"values collapsed into fewer keys": {
			vals:   []uint64{0, 1 << 16, 2 << 16},
			mutate: func(vals []uint64) { vals[1], vals[2] = 1, 2 },
			expect: []uint64{0, 1, 2},
		},
	}
	for name, tc := range tolerated {
		t.Run(name, func(t *testing.T) {
			var bm *Bitmap
			require.NotPanics(t, func() {
				bm = FromSortedListToBuf(tc.vals, func(sizeBytes int) []byte {
					tc.mutate(tc.vals)
					return make([]byte, sizeBytes)
				})
			})
			require.Equal(t, tc.expect, bm.ToArray())
			require.Equal(t, tc.expect, FromBuffer(bm.ToBuffer()).ToArray())

			direct := FromSortedList(tc.expect).ToBuffer()
			if tc.canonical {
				require.Equal(t, direct, bm.ToBuffer())
			} else {
				require.NotEqual(t, direct, bm.ToBuffer())
			}
		})
	}

	// Duplicates only ever make the requested size an upper bound, so they
	// stay in the buffer rather than tripping the check.
	t.Run("duplicates do not trip the check", func(t *testing.T) {
		vals := make([]uint64, 0, 2049)
		for i := 0; i < 2049; i++ {
			vals = append(vals, uint64(i/683))
		}
		var bm *Bitmap
		require.NotPanics(t, func() {
			bm = FromSortedListToBuf(vals, func(sizeBytes int) []byte {
				return make([]byte, sizeBytes)
			})
		})
		require.Equal(t, []uint64{0, 1, 2}, bm.ToArray())
		require.NotNil(t, bm._ptr)
	})
}

// makeMaskedEntriesForSearch builds entries whose masked keys are distinct, so
// "first index >= target" has a single unambiguous answer.
func makeMaskedEntriesForSearch(numKeys int) []keyedMaskedEntry {
	bm := NewBitmap()
	for i := 0; i < numKeys; i++ {
		bm.Set(uint64(i+1) << 16)
	}
	return buildKeyedMaskedEntries(bm, ^uint64(0))
}

func linearMaskedSearchFrom(entries []keyedMaskedEntry, from int, target uint64) int {
	lower := from + 1
	if lower >= len(entries) {
		return lower
	}
	for i := lower; i < len(entries); i++ {
		if entries[i].maskedKey >= target {
			return i
		}
	}
	return len(entries)
}

func TestSearchKeyedMaskedFrom(t *testing.T) {
	t.Run("from -1 can land on index 0", func(t *testing.T) {
		e := makeMaskedEntriesForSearch(10)
		require.Equal(t, 0, searchKeyedMaskedFrom(e, -1, e[0].maskedKey))
	})

	t.Run("gap of 1 returns from+1 immediately", func(t *testing.T) {
		e := makeMaskedEntriesForSearch(10)
		require.Equal(t, 1, searchKeyedMaskedFrom(e, 0, e[1].maskedKey))
	})

	t.Run("exact match within range", func(t *testing.T) {
		e := makeMaskedEntriesForSearch(100)
		for _, from := range []int{-1, 0, 10, 50} {
			for target := from + 1; target < len(e); target++ {
				require.Equal(t, target, searchKeyedMaskedFrom(e, from, e[target].maskedKey),
					"from=%d target=%d", from, target)
			}
		}
	})

	t.Run("between two keys returns first key >= target", func(t *testing.T) {
		e := makeMaskedEntriesForSearch(50)
		require.Equal(t, 6, searchKeyedMaskedFrom(e, 4, e[5].maskedKey+1))
	})

	t.Run("target beyond all keys returns len", func(t *testing.T) {
		e := makeMaskedEntriesForSearch(20)
		require.Equal(t, len(e), searchKeyedMaskedFrom(e, 0, e[len(e)-1].maskedKey+1))
	})

	t.Run("from+1 >= len returns from+1", func(t *testing.T) {
		e := makeMaskedEntriesForSearch(5)
		last := len(e) - 1
		require.Equal(t, len(e), searchKeyedMaskedFrom(e, last, e[last].maskedKey+1))
	})

	t.Run("large gap exercises the exponential path", func(t *testing.T) {
		e := makeMaskedEntriesForSearch(1000)
		require.Equal(t, len(e)-1, searchKeyedMaskedFrom(e, -1, e[len(e)-1].maskedKey))
		require.Equal(t, 901, searchKeyedMaskedFrom(e, 0, e[900].maskedKey+1))
	})

	t.Run("agrees with a linear walk across positions and targets", func(t *testing.T) {
		e := makeMaskedEntriesForSearch(200)
		for from := -1; from < len(e); from++ {
			for _, offset := range []int{1, 2, 8, 16, 64, 128} {
				target := from + offset
				if target >= len(e) {
					break
				}
				for _, k := range []uint64{
					e[target].maskedKey, e[target].maskedKey + 1, e[target].maskedKey - 1,
				} {
					require.Equal(t, linearMaskedSearchFrom(e, from, k),
						searchKeyedMaskedFrom(e, from, k), "from=%d k=%d", from, k)
				}
			}
		}
	})
}
