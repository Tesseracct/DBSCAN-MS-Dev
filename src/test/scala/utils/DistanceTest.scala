package utils

import org.scalatest.funsuite.AnyFunSuite

class DistanceTest extends AnyFunSuite {
  test("Euclidean distance between two points") {
    val a = Array(1.0f, 2.0f, 3.0f)
    val b = Array(4.0f, 4.0f, 6.0f)
    val expected = math.sqrt(22.0).toFloat // sqrt((4-1)^2 + (4-2)^2 + (6-3)^2)

    assert(Distance.euclidean(a, b) == expected)
    assert(Math.abs(Distance.euclidean(a, b) - expected) < 1e-6)
  }

  test("Euclidean distance with negative coordinates") {
    val a = Array(-1.0f, -2.0f)
    val b = Array(-4.0f, -6.0f)
    val expected = math.sqrt(25.0).toFloat // sqrt((-4+1)^2 + (-6+2)^2)

    assert(Distance.euclidean(a, b) == expected)
    assert(Math.abs(Distance.euclidean(a, b) - expected) < 1e-6)
  }

  test("Euclidean distance with different lengths throws exception") {
    val a = Array(1.0f, 2.0f)
    val b = Array(3.0f, 4.0f, 5.0f)

    assertThrows[IllegalArgumentException] {
      Distance.euclidean(a, b)
    }
  }

  test("Euclidean distance with same points is zero") {
    val a = Array(1.0f, 2.0f)
    assert(Distance.euclidean(a, a) == 0.0)
  }

}
