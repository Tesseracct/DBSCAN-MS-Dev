package utils

import org.scalatest.funsuite.AnyFunSuite
import scala.util.Random

class QuickselectTest extends AnyFunSuite {

  test("Select smallest element") {
    val arr = Array(5f, 3f, 8f, 1f, 9f)
    val result = Quickselect.select(arr.clone(), 0)
    assert(result == 1f)
  }

  test("Select largest element") {
    val arr = Array(5f, 3f, 8f, 1f, 9f)
    val result = Quickselect.select(arr.clone(), arr.length - 1)
    assert(result == 9f)
  }

  test("Select median element (odd length)") {
    val arr = Array(7f, 2f, 9f, 4f, 1f)
    val sorted = arr.sorted
    val k = arr.length / 2
    val result = Quickselect.select(arr.clone(), k)
    assert(result == sorted(k))
  }

  test("Select median element (even length)") {
    val arr = Array(8f, 4f, 3f, 6f)
    val sorted = arr.sorted
    val k = arr.length / 2
    val result = Quickselect.select(arr.clone(), k)
    assert(result == sorted(k))
  }

  test("Handle duplicate elements correctly") {
    val arr = Array(5f, 1f, 5f, 1f, 5f)
    val sorted = arr.sorted
    for (k <- arr.indices) {
      val result = Quickselect.select(arr.clone(), k)
      assert(result == sorted(k))
    }
  }

  test("Single-element array") {
    val arr = Array(42f)
    val result = Quickselect.select(arr.clone(), 0)
    assert(result == 42f)
  }

  test("Already sorted input") {
    val arr = Array(1f, 2f, 3f, 4f, 5f)
    for (k <- arr.indices) {
      val result = Quickselect.select(arr.clone(), k)
      assert(result == arr(k))
    }
  }

  test("Reverse-sorted input") {
    val arr = Array(9f, 8f, 7f, 6f, 5f)
    val sorted = arr.sorted
    for (k <- arr.indices) {
      val result = Quickselect.select(arr.clone(), k)
      assert(result == sorted(k))
    }
  }

  test("Randomized input consistency") {
    val rand = new Random(42)
    for (_ <- 1 to 100) {
      val arr = Array.fill(50)(rand.nextFloat() * 1000)
      val sorted = arr.sorted
      val k = rand.nextInt(arr.length)
      val result = Quickselect.select(arr.clone(), k)
      assert(result == sorted(k))
    }
  }

  test("Large input array") {
    val arr = (1 to 1000000).map(_.toFloat).toArray
    val k = 543210
    val result = Quickselect.select(arr.clone(), k)
    assert(result == (k + 1).toFloat)
  }

  test("Negative numbers and mixed values") {
    val arr = Array(-5f, -10f, 0f, 5f, 10f)
    val sorted = arr.sorted
    for (k <- arr.indices) {
      val result = Quickselect.select(arr.clone(), k)
      assert(result == sorted(k))
    }
  }
}
