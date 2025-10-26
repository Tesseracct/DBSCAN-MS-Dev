package utils

import scala.util.Random

object Quickselect {
  def select(arr: Array[Float], k: Int): Float = {
    require(k >= 0 && k < arr.length, s"Index $k out of bounds for array of length ${arr.length}")

    var left = 0
    var right = arr.length - 1
    val rand = new Random(42)

    while (left < right) {
      val pivot = arr(left + rand.nextInt(right - left + 1))
      val (i, j) = partition(arr, left, right, pivot)

      if (k <= j) right = j
      else if (k >= i) left = i
      else return arr(k)
    }
    arr(k)
  }

  private def partition(arr: Array[Float], left: Int, right: Int, pivot: Float): (Int, Int) = {
    var i = left
    var j = right
    while (i <= j) {
      while (arr(i) < pivot) i += 1
      while (arr(j) > pivot) j -= 1
      if (i <= j) {
        val temp = arr(i)
        arr(i) = arr(j)
        arr(j) = temp
        i += 1
        j -= 1
      }
    }
    (i, j)
  }
}
