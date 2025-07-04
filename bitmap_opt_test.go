package sroar

import (
	"fmt"
	"math/bits"
	"math/rand"
	"slices"
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

func TestCompareNumKeys(t *testing.T) {
	var bmNil *Bitmap

	bm1Key := NewBitmap()
	bm1Key.Set(1)

	bm2Keys := NewBitmap()
	bm2Keys.Set(1)
	bm2Keys.Set(1 + uint64(maxCardinality))

	bm3Keys := NewBitmap()
	bm3Keys.Set(1)
	bm3Keys.Set(1 + uint64(maxCardinality))
	bm3Keys.Set(1 + uint64(maxCardinality)*2)

	t.Run("greater", func(t *testing.T) {
		for _, bms := range [][2]*Bitmap{
			{bm1Key, bmNil},
			{bm2Keys, bmNil},
			{bm2Keys, bm1Key},
			{bm3Keys, bmNil},
			{bm3Keys, bm1Key},
			{bm3Keys, bm2Keys},
		} {
			require.Equal(t, 1, bms[0].CompareNumKeys(bms[1]))
		}
	})

	t.Run("equal", func(t *testing.T) {
		for _, bms := range [][2]*Bitmap{
			{bmNil, bmNil},
			{bm1Key, bm1Key},
			{bm2Keys, bm2Keys},
			{bm3Keys, bm3Keys},
		} {
			require.Equal(t, 0, bms[0].CompareNumKeys(bms[1]))
		}
	})

	t.Run("less", func(t *testing.T) {
		for _, bms := range [][2]*Bitmap{
			{bmNil, bm1Key},
			{bmNil, bm2Keys},
			{bmNil, bm3Keys},
			{bm1Key, bm2Keys},
			{bm1Key, bm3Keys},
			{bm2Keys, bm3Keys},
		} {
			require.Equal(t, -1, bms[0].CompareNumKeys(bms[1]))
		}
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
	})

	t.Run("panic on smaller buffer size", func(t *testing.T) {
		defer func() {
			r := recover()
			require.NotNil(t, r)
			require.Contains(t, r, "Buffer too small")
		}()

		bm := NewBitmap()
		bm.Set(1)
		bmLen := bm.LenInBytes()

		buf := make([]byte, 0, bmLen-1)
		bm.CloneToBuf(buf)
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
		bm := newBitmapWith(
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
