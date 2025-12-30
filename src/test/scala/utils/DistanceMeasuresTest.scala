package utils

import org.scalatest.funsuite.AnyFunSuite

class DistanceMeasuresTest extends AnyFunSuite {
  test("Euclidean distance between two points") {
    val a = Array(1.0f, 2.0f, 3.0f)
    val b = Array(4.0f, 4.0f, 6.0f)
    val expected = math.sqrt(22.0).toFloat // sqrt((4-1)^2 + (4-2)^2 + (6-3)^2)

    assert(DistanceMeasures.euclidean(a, b) == expected)
    assert(Math.abs(DistanceMeasures.euclidean(a, b) - expected) < 1e-6)
  }

  test("Euclidean distance with negative coordinates") {
    val a = Array(-1.0f, -2.0f)
    val b = Array(-4.0f, -6.0f)
    val expected = math.sqrt(25.0).toFloat // sqrt((-4+1)^2 + (-6+2)^2)

    assert(DistanceMeasures.euclidean(a, b) == expected)
    assert(Math.abs(DistanceMeasures.euclidean(a, b) - expected) < 1e-6)
  }

  test("Euclidean distance with same points is zero") {
    val a = Array(1.0f, 2.0f)
    assert(DistanceMeasures.euclidean(a, a) == 0.0)
  }

  // Levenshtein distance tests
  test("returns 0 for identical strings (fast-path)") {
    assert(DistanceMeasures.levenshtein("abc", "abc") == 0)
    assert(DistanceMeasures.levenshtein("", "") == 0)
  }

  test("handles empty strings (fast-path)") {
    assert(DistanceMeasures.levenshtein("", "abc") == 3)
    assert(DistanceMeasures.levenshtein("abc", "") == 3)
  }

  test("single edit operations: insert, delete, substitute") {
    // insert / delete
    assert(DistanceMeasures.levenshtein("abc", "abxc") == 1)
    assert(DistanceMeasures.levenshtein("abxc", "abc") == 1)

    // substitute
    assert(DistanceMeasures.levenshtein("abc", "axc") == 1)
  }

  test("classic known examples") {
    assert(DistanceMeasures.levenshtein("kitten", "sitting") == 3)
    assert(DistanceMeasures.levenshtein("flaw", "lawn") == 2)
    assert(DistanceMeasures.levenshtein("gumbo", "gambol") == 2)
  }

  test("symmetry: d(a,b) == d(b,a) (stresses longer/shorter swap)") {
    val a = "abcdef"
    val b = "azced"
    assert(DistanceMeasures.levenshtein(a, b) == DistanceMeasures.levenshtein(b, a))
  }

  test("bounds: 0 <= d(a,b) <= max(len(a), len(b))") {
    val cases = Seq(
      ("", "", 0),
      ("a", "", 1),
      ("", "abcd", 4),
      ("abc", "xyz123", DistanceMeasures.levenshtein("abc", "xyz123"))
    )

    cases.foreach { case (a, b, d) =>
      assert(d >= 0)
      assert(d <= math.max(a.length, b.length))
    }
  }

  test("triangle inequality spot-check: d(a,c) <= d(a,b) + d(b,c)") {
    val a = "kitten"
    val b = "sitting"
    val c = "smitten"
    assert(DistanceMeasures.levenshtein(a, c) <= DistanceMeasures.levenshtein(a, b) + DistanceMeasures.levenshtein(b, c))
  }

  test("repeated characters and off-by-one stress") {
    assert(DistanceMeasures.levenshtein("aaaaa", "aaa") == 2)
    assert(DistanceMeasures.levenshtein("aaa", "aaaaa") == 2)
    assert(DistanceMeasures.levenshtein("aaaa", "bbbb") == 4)
  }

  test("length-difference lower bound: |len(a)-len(b)| <= d(a,b)") {
    val pairs = Seq(
      ("a", "abcd"),
      ("abcdef", "ab"),
      ("", "xyz"),
      ("same", "same"),
      ("hello", "yellow")
    )

    pairs.foreach { case (a, b) =>
      val d = DistanceMeasures.levenshtein(a, b)
      assert(d >= math.abs(a.length - b.length))
    }
  }
}
