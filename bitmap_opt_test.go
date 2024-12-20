package sroar

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareMergeImplementations(t *testing.T) {
	randSeed := int64(1724861525311)
	rnd := rand.New(rand.NewSource(randSeed))
	buf := make([]uint16, maxContainerSize)

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
			and2 := dst.Clone()
			and3 := dst.Clone()

			and1.AndOld(src)
			and2.And(src)
			and3.AndBuf(src, buf)
			and4 := AndOld(dst, src)
			and5 := And(dst, src)
			and6 := AndBuf(dst, src, buf)

			require.Equal(t, expCardinality, and1.GetCardinality())
			require.Equal(t, expCardinality, and2.GetCardinality())
			require.Equal(t, expCardinality, and3.GetCardinality())
			require.Equal(t, expCardinality, and4.GetCardinality())
			require.Equal(t, expCardinality, and5.GetCardinality())
			require.Equal(t, expCardinality, and6.GetCardinality())

			if match {
				expElements := and1.ToArray()
				require.ElementsMatch(t, expElements, and2.ToArray())
				require.ElementsMatch(t, expElements, and3.ToArray())
				require.ElementsMatch(t, expElements, and4.ToArray())
				require.ElementsMatch(t, expElements, and5.ToArray())
				require.ElementsMatch(t, expElements, and6.ToArray())
			}
		}
		runMatch := func(t *testing.T, dst, src *Bitmap, expCardinality int) {
			run(t, dst, src, expCardinality, true)
		}
		runNoMatch := func(t *testing.T, dst, src *Bitmap, expCardinality int) {
			run(t, dst, src, expCardinality, false)
		}

		runMatch(t, bmA, bmB, 3675)
		runMatch(t, bmA, bmC, 3693)
		runMatch(t, bmA, bmD, 3627)
		runMatch(t, bmA, bmE, 3730)
		runMatch(t, bmA, bmF, 932)

		runMatch(t, bmB, bmA, 3675)
		runMatch(t, bmB, bmC, 3689)
		runMatch(t, bmB, bmD, 3676)
		runMatch(t, bmB, bmE, 882)
		runMatch(t, bmB, bmF, 3601)

		runMatch(t, bmC, bmA, 3693)
		runMatch(t, bmC, bmB, 3689)
		runMatch(t, bmC, bmD, 928)
		runMatch(t, bmC, bmE, 3701)
		runMatch(t, bmC, bmF, 3610)

		runMatch(t, bmD, bmA, 3627)
		runMatch(t, bmD, bmB, 3676)
		runMatch(t, bmD, bmC, 928)
		runMatch(t, bmD, bmE, 3666)
		runMatch(t, bmD, bmF, 3654)

		runMatch(t, bmE, bmA, 3730)
		runMatch(t, bmE, bmB, 882)
		runMatch(t, bmE, bmC, 3701)
		runMatch(t, bmE, bmD, 3666)
		runMatch(t, bmE, bmF, 3674)

		runMatch(t, bmF, bmA, 932)
		runMatch(t, bmF, bmB, 3601)
		runMatch(t, bmF, bmC, 3610)
		runMatch(t, bmF, bmD, 3654)
		runMatch(t, bmF, bmE, 3674)

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

		runMatch(t, bigA, bmA, 3407)
		runMatch(t, bigA, bmB, 3349)
		runMatch(t, bigA, bmC, 3307)
		runMatch(t, bigA, bmD, 3360)
		runMatch(t, bigA, bmE, 3413)
		runMatch(t, bigA, bmF, 3331)

		runMatch(t, bmA, bigA, 3407)
		runMatch(t, bmB, bigA, 3349)
		runMatch(t, bmC, bigA, 3307)
		runMatch(t, bmD, bigA, 3360)
		runMatch(t, bmE, bigA, 3413)
		runMatch(t, bmF, bigA, 3331)

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
			andNot2 := dst.Clone()
			andNot3 := dst.Clone()

			andNot1.AndNotOld(src)
			andNot2.AndNot(src)
			andNot3.AndNotBuf(src, buf)
			andNot4 := AndNot(dst, src)
			andNot5 := AndNotBuf(dst, src, buf)

			require.Equal(t, expCardinality, andNot1.GetCardinality())
			require.Equal(t, expCardinality, andNot2.GetCardinality())
			require.Equal(t, expCardinality, andNot3.GetCardinality())
			require.Equal(t, expCardinality, andNot4.GetCardinality())
			require.Equal(t, expCardinality, andNot5.GetCardinality())

			if match {
				expElements := andNot1.ToArray()
				require.ElementsMatch(t, expElements, andNot2.ToArray())
				require.ElementsMatch(t, expElements, andNot3.ToArray())
				require.ElementsMatch(t, expElements, andNot4.ToArray())
				require.ElementsMatch(t, expElements, andNot5.ToArray())
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
		runNoMatch(t, bmA, bmF, 30074)

		runNoMatch(t, bmB, bmA, 27320)
		runNoMatch(t, bmB, bmC, 27306)
		runNoMatch(t, bmB, bmD, 27319)
		runNoMatch(t, bmB, bmE, 30113)
		runNoMatch(t, bmB, bmF, 27394)

		runNoMatch(t, bmC, bmA, 27322)
		runNoMatch(t, bmC, bmB, 27326)
		runNoMatch(t, bmC, bmD, 30087)
		runNoMatch(t, bmC, bmE, 27314)
		runNoMatch(t, bmC, bmF, 27405)

		runNoMatch(t, bmD, bmA, 27464)
		runNoMatch(t, bmD, bmB, 27415)
		runNoMatch(t, bmD, bmC, 30163)
		runNoMatch(t, bmD, bmE, 27425)
		runNoMatch(t, bmD, bmF, 27437)

		runNoMatch(t, bmE, bmA, 27237)
		runNoMatch(t, bmE, bmB, 30085)
		runNoMatch(t, bmE, bmC, 27266)
		runNoMatch(t, bmE, bmD, 27301)
		runNoMatch(t, bmE, bmF, 27293)

		runNoMatch(t, bmF, bmA, 30153)
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

		runMatch(t, bmA, superset, 0)
		runMatch(t, bmB, superset, 0)
		runMatch(t, bmC, superset, 0)
		runMatch(t, bmD, superset, 0)
		runMatch(t, bmE, superset, 0)
		runMatch(t, bmF, superset, 0)

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

		runMatch(t, bmA, bigB, 945)
		runMatch(t, bmB, bigB, 989)
		runMatch(t, bmC, bigB, 923)
		runMatch(t, bmD, bigB, 937)
		runMatch(t, bmE, bigB, 971)
		runMatch(t, bmF, bigB, 988)
	})

	t.Run("or", func(t *testing.T) {
		run := func(t *testing.T, dst, src *Bitmap, expCardinality int, match bool) {
			or1 := dst.Clone()
			or2 := dst.Clone()
			or3 := dst.Clone()

			or1.OrOld(src)
			or2.Or(src)
			or3.OrBuf(src, buf)
			or4 := OrOld(dst, src)
			or5 := Or(dst, src)
			or6 := OrBuf(dst, src, buf)

			require.Equal(t, expCardinality, or1.GetCardinality())
			require.Equal(t, expCardinality, or2.GetCardinality())
			require.Equal(t, expCardinality, or3.GetCardinality())
			require.Equal(t, expCardinality, or4.GetCardinality())
			require.Equal(t, expCardinality, or5.GetCardinality())
			require.Equal(t, expCardinality, or6.GetCardinality())

			if match {
				expElements := or1.ToArray()
				require.ElementsMatch(t, expElements, or2.ToArray())
				require.ElementsMatch(t, expElements, or3.ToArray())
				require.ElementsMatch(t, expElements, or4.ToArray())
				require.ElementsMatch(t, expElements, or5.ToArray())
				require.ElementsMatch(t, expElements, or6.ToArray())
			}
		}
		runNoMatch := func(t *testing.T, dst, src *Bitmap, expCardinality int) {
			run(t, dst, src, expCardinality, false)
		}

		runNoMatch(t, bmA, bmB, 58326)
		runNoMatch(t, bmA, bmC, 58328)
		runNoMatch(t, bmA, bmD, 58470)
		runNoMatch(t, bmA, bmE, 58243)
		runNoMatch(t, bmA, bmF, 61159)

		runNoMatch(t, bmB, bmA, 58326)
		runNoMatch(t, bmB, bmC, 58321)
		runNoMatch(t, bmB, bmD, 58410)
		runNoMatch(t, bmB, bmE, 61080)
		runNoMatch(t, bmB, bmF, 58479)

		runNoMatch(t, bmC, bmA, 58328)
		runNoMatch(t, bmC, bmB, 58321)
		runNoMatch(t, bmC, bmD, 61178)
		runNoMatch(t, bmC, bmE, 58281)
		runNoMatch(t, bmC, bmF, 58490)

		runNoMatch(t, bmD, bmA, 58470)
		runNoMatch(t, bmD, bmB, 58410)
		runNoMatch(t, bmD, bmC, 61178)
		runNoMatch(t, bmD, bmE, 58392)
		runNoMatch(t, bmD, bmF, 58522)

		runNoMatch(t, bmE, bmA, 58243)
		runNoMatch(t, bmE, bmB, 61080)
		runNoMatch(t, bmE, bmC, 58281)
		runNoMatch(t, bmE, bmD, 58392)
		runNoMatch(t, bmE, bmF, 58378)

		runNoMatch(t, bmF, bmA, 61159)
		runNoMatch(t, bmF, bmB, 58479)
		runNoMatch(t, bmF, bmC, 58490)
		runNoMatch(t, bmF, bmD, 58522)
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
			seq2 := dst.Clone()
			seq3 := dst.Clone()
			var seq4, seq5, seq6 *Bitmap

			seq1.OrOld(a)
			seq1.AndOld(b)
			seq1.AndNotOld(c)
			seq1.OrOld(d)
			seq1.AndOld(e)
			seq1.AndNotOld(f)

			seq2.Or(a).And(b).AndNot(c).Or(d).And(e).AndNot(f)

			seq3.OrBuf(a, buf).AndBuf(b, buf).AndNotBuf(c, buf).OrBuf(d, buf).AndBuf(e, buf).AndNotBuf(f, buf)

			seq4 = OrOld(dst, a)
			seq4 = AndOld(seq4, b)
			seq4.AndNotOld(c)
			seq4 = OrOld(seq4, d)
			seq4 = AndOld(seq4, e)
			seq4.AndNotOld(f)

			seq5 = Or(dst, a)
			seq5 = And(seq5, b)
			seq5 = AndNot(seq5, c)
			seq5 = Or(seq5, d)
			seq5 = And(seq5, e)
			seq5 = AndNot(seq5, f)

			seq6 = OrBuf(dst, a, buf)
			seq6 = AndBuf(seq6, b, buf)
			seq6 = AndNotBuf(seq6, c, buf)
			seq6 = OrBuf(seq6, d, buf)
			seq6 = AndBuf(seq6, e, buf)
			seq6 = AndNotBuf(seq6, f, buf)

			require.Equal(t, expCardinality, seq1.GetCardinality())
			require.Equal(t, expCardinality, seq2.GetCardinality())
			require.Equal(t, expCardinality, seq3.GetCardinality())
			require.Equal(t, expCardinality, seq4.GetCardinality())
			require.Equal(t, expCardinality, seq5.GetCardinality())
			require.Equal(t, expCardinality, seq6.GetCardinality())

			if match {
				expElements := seq1.ToArray()
				require.ElementsMatch(t, expElements, seq2.ToArray())
				require.ElementsMatch(t, expElements, seq3.ToArray())
				require.ElementsMatch(t, expElements, seq4.ToArray())
				require.ElementsMatch(t, expElements, seq5.ToArray())
				require.ElementsMatch(t, expElements, seq6.ToArray())
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
			return AndOld(aa, bb).GetCardinality()
		}
		and3Card := func(aa, bb *Bitmap) int {
			return aa.Clone().And(bb).GetCardinality()
		}
		and4Card := func(aa, bb *Bitmap) int {
			return And(aa, bb).GetCardinality()
		}
		and5Card := func(aa, bb *Bitmap) int {
			return aa.Clone().AndBuf(bb, buf).GetCardinality()
		}
		and6Card := func(aa, bb *Bitmap) int {
			return AndBuf(aa, bb, buf).GetCardinality()
		}

		andNot1Card := func(aa, bb *Bitmap) int {
			aa = aa.Clone()
			aa.AndNotOld(bb)
			return aa.GetCardinality()
		}
		andNot2Card := func(aa, bb *Bitmap) int {
			return aa.Clone().AndNot(bb).GetCardinality()
		}
		andNot3Card := func(aa, bb *Bitmap) int {
			return AndNot(aa, bb).GetCardinality()
		}
		andNot4Card := func(aa, bb *Bitmap) int {
			return aa.Clone().AndNotBuf(bb, buf).GetCardinality()
		}
		andNot5Card := func(aa, bb *Bitmap) int {
			return AndNotBuf(aa, bb, buf).GetCardinality()
		}

		or1Card := func(aa, bb *Bitmap) int {
			aa = aa.Clone()
			aa.OrOld(bb)
			return aa.GetCardinality()
		}
		or2Card := func(aa, bb *Bitmap) int {
			return OrOld(aa, bb).GetCardinality()
		}
		or3Card := func(aa, bb *Bitmap) int {
			return aa.Clone().Or(bb).GetCardinality()
		}
		or4Card := func(aa, bb *Bitmap) int {
			return Or(aa, bb).GetCardinality()
		}
		or5Card := func(aa, bb *Bitmap) int {
			return aa.Clone().OrBuf(bb, buf).GetCardinality()
		}
		or6Card := func(aa, bb *Bitmap) int {
			return OrBuf(aa, bb, buf).GetCardinality()
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
				require.Equal(t, andCard, and3Card(a, b))
				require.Equal(t, andCard, and4Card(a, b))
				require.Equal(t, andCard, and5Card(a, b))
				require.Equal(t, andCard, and6Card(a, b))
			})

			t.Run("andNot card", func(t *testing.T) {
				require.Equal(t, andNotACard, andNot1Card(a, b))
				require.Equal(t, andNotACard, andNot2Card(a, b))
				require.Equal(t, andNotACard, andNot3Card(a, b))
				require.Equal(t, andNotACard, andNot4Card(a, b))
				require.Equal(t, andNotACard, andNot5Card(a, b))
			})

			t.Run("or card", func(t *testing.T) {
				require.Equal(t, orCard, or1Card(a, b))
				require.Equal(t, orCard, or2Card(a, b))
				require.Equal(t, orCard, or3Card(a, b))
				require.Equal(t, orCard, or4Card(a, b))
				require.Equal(t, orCard, or5Card(a, b))
				require.Equal(t, orCard, or6Card(a, b))
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

func TestMergeToSuperset(t *testing.T) {
	run := func(t *testing.T, bufs [][]uint16) {
		containerThreshold := uint64(math.MaxUint16 + 1)

		// containers of type array + bitmap + bitmap
		superset := NewBitmap()
		// containers of type array + array + bitmap
		and := NewBitmap()
		or := NewBitmap()
		andNot := NewBitmap()

		t.Run("init bitmaps", func(t *testing.T) {
			N1 := uint64(4000)  // fits to array container
			N2 := uint64(16000) // fits to bitmap container

			// containers of type array for all BMs
			for i := uint64(0); i < N1; i++ {
				val1 := i * 2

				superset.Set(val1)
				if i%3 != 0 {
					and.Set(i)
				}
				if i < N1*3/4 {
					or.Set(i)
				}
				if i%2 == 0 {
					andNot.Set(i)
				}
			}

			// containers of type 2xbitmap for superset
			// containers of type array+bitmap for subsets
			for i := uint64(0); i < N2; i++ {
				val2 := i*3 + containerThreshold
				val3 := i*4 + 2*containerThreshold

				superset.Set(val2)
				superset.Set(val3)

				if i%5 == 1 {
					and.Set(val2)
				}
				if a := i % 11; a == 3 || a == 7 {
					or.Set(val2)
				}
				if a := i % 23; a < 5 {
					andNot.Set(val2)
				}

				if a := i % 7; a > 3 {
					and.Set(val3)
				}
				if a := i % 13; a < 10 {
					or.Set(val3)
				}
				if a := i % 17; a > 2 && a < 15 {
					andNot.Set(val3)
				}
			}
		})

		control := superset.Clone()

		t.Run("and", func(t *testing.T) {
			control.And(and)
			superset.AndToSuperset(and, bufs...)

			require.Equal(t, 11389, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("or", func(t *testing.T) {
			control.Or(or)
			superset.OrToSuperset(or, bufs...)

			require.Equal(t, 22750, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("and not", func(t *testing.T) {
			control.AndNot(andNot)
			superset.AndNotToSuperset(andNot, bufs...)

			require.Equal(t, 9911, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("2nd or", func(t *testing.T) {
			control.Or(or)
			superset.OrToSuperset(or, bufs...)

			require.Equal(t, 20730, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("2nd and", func(t *testing.T) {
			control.And(and)
			superset.AndToSuperset(and, bufs...)

			require.Equal(t, 10369, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("2nd and not", func(t *testing.T) {
			control.AndNot(andNot)
			superset.AndNotToSuperset(andNot, bufs...)

			require.Equal(t, 5520, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())
		})

		t.Run("merge into", func(t *testing.T) {
			dst := NewBitmap()
			for _, val1 := range []uint64{0123, 1234, 2345, 3456, 4567, 5678, 6789, 7890, 8901, 9012} {
				val2 := val1 + containerThreshold
				val3 := val1 + 2*containerThreshold

				superset.Set(val1)
				superset.Set(val2)
				superset.Set(val3)
				control.Set(val1)
				control.Set(val2)
				control.Set(val3)

				dst.Set(val1)
				dst.Set(val2)
				dst.Set(val3)
			}
			controlDst := dst.Clone()

			require.Equal(t, 5548, superset.GetCardinality())
			require.ElementsMatch(t, control.ToArray(), superset.ToArray())

			dst.And(superset)
			controlDst.And(control)

			require.Equal(t, 30, dst.GetCardinality())
			require.ElementsMatch(t, controlDst.ToArray(), dst.ToArray())

			dst.Or(superset)
			controlDst.Or(control)

			require.Equal(t, 5548, dst.GetCardinality())
			require.ElementsMatch(t, controlDst.ToArray(), dst.ToArray())
		})
	}

	t.Run("single buffer", func(t *testing.T) {
		run(t, makeContainerBuffers(1))
	})

	t.Run("multiple buffers (concurrent)", func(t *testing.T) {
		run(t, makeContainerBuffers(4))
	})
}

// go test -v -fuzz FuzzMergeToSuperset -fuzztime 600s -run ^$ github.com/weaviate/sroar
func FuzzMergeToSuperset(f *testing.F) {
	type testCase struct {
		name          string
		countElements int
		countSubsets  int
		countMerges   int
		countBuffers  int
		randSeed      int64
	}

	testCases := []testCase{
		{
			name:          "few elements, few subsets",
			countElements: 1_000,
			countSubsets:  3,
			countMerges:   15,
			countBuffers:  1,
			randSeed:      1724861525311406000,
		},
		{
			name:          "few elements, many subsets",
			countElements: 2_000,
			countSubsets:  15,
			countMerges:   14,
			countBuffers:  2,
			randSeed:      172486152531140600,
		},
		{
			name:          "more elements, few subsets",
			countElements: 5_000,
			countSubsets:  4,
			countMerges:   13,
			countBuffers:  3,
			randSeed:      17248615253114060,
		},
		{
			name:          "more elements, many subsets",
			countElements: 7_000,
			countSubsets:  16,
			countMerges:   12,
			countBuffers:  4,
			randSeed:      1724861525311406,
		},
		{
			name:          "many elements, few subsets",
			countElements: 19_000,
			countSubsets:  5,
			countMerges:   11,
			countBuffers:  5,
			randSeed:      172486152531140,
		},
		{
			name:          "many elements, many subsets",
			countElements: 25_000,
			countSubsets:  18,
			countMerges:   10,
			countBuffers:  6,
			randSeed:      17248615253114,
		},
	}

	for _, tc := range testCases {
		f.Add(tc.countElements, tc.countSubsets, tc.countMerges, tc.countBuffers, tc.randSeed)
	}

	f.Fuzz(runMergeToSuperSetTest)
}

func TestMergeToSuperset_VerifyFuzzCallback(t *testing.T) {
	t.Run("single buffer", func(t *testing.T) {
		runMergeToSuperSetTest(t, 23_456, 17, 9, 1, 1724861525311)
	})

	t.Run("multiple buffers (concurrent)", func(t *testing.T) {
		runMergeToSuperSetTest(t, 23_456, 17, 9, 4, 1724861525311)
	})
}

func runMergeToSuperSetTest(t *testing.T,
	countElements, countSubsets, countMerges, countBuffers int, randSeed int64,
) {
	if countElements < 100 || countElements > 50_000 {
		return
	}
	if countSubsets < 1 || countSubsets > 25 {
		return
	}
	if countMerges < 1 || countMerges > 50 {
		return
	}
	if countBuffers < 1 || countBuffers > 32 {
		return
	}

	// max element is 3x bigger than capacity of single bm's container
	maxX := (int(math.MaxUint16) + 1) * 3
	buffers := makeContainerBuffers(countBuffers)
	rnd := rand.New(rand.NewSource(randSeed))

	superset := NewBitmap()
	subsets := make([]*Bitmap, countSubsets)
	var control *Bitmap

	t.Run("populate bitmaps", func(t *testing.T) {
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

		control = superset.Clone()
	})

	for i := 0; i < countMerges; i++ {
		t.Run("merge bitmaps", func(t *testing.T) {
			id := rnd.Intn(len(subsets))
			subset := subsets[id]

			switch mergeType := rnd.Intn(3); mergeType {
			case 1:
				t.Run(fmt.Sprintf("AND with %d", id), func(t *testing.T) {
					superset.AndToSuperset(subset, buffers...)
					control.And(subset)
					assertMatches(t, superset, control)
				})
			case 2:
				t.Run(fmt.Sprintf("AND NOT with %d", id), func(t *testing.T) {
					superset.AndNotToSuperset(subset, buffers...)
					control.AndNot(subset)
					assertMatches(t, superset, control)
				})
			default:
				t.Run(fmt.Sprintf("OR with %d", id), func(t *testing.T) {
					superset.OrToSuperset(subset, buffers...)
					control.Or(subset)
					assertMatches(t, superset, control)
				})
			}
		})
	}
}

func assertMatches(t *testing.T, bm1, bm2 *Bitmap) {
	require.Equal(t, bm1.GetCardinality(), bm2.GetCardinality())

	// check elements match using iterator as
	// require.ElementsMatch(t, bm1.ToArray(), bm2.ToArray())
	// causes fuzz test to fail frequently
	cit := bm1.NewIterator()
	sit := bm2.NewIterator()
	for {
		cx := cit.Next()
		sx := sit.Next()
		require.Equal(t, cx, sx)

		if cx == 0 || sx == 0 {
			break
		}
	}
}

func makeContainerBuffers(n int) [][]uint16 {
	bufs := make([][]uint16, n)
	for i := range bufs {
		bufs[i] = make([]uint16, maxContainerSize)
	}
	return bufs
}
