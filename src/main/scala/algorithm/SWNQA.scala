package algorithm

import model.DataPoint
import utils.TinyArrayBuffer

import scala.collection.mutable.ArrayBuffer


object SWNQA {
  /**
   * Computes the neighbourhoods of a given set of data points.
   * @param points The data points sorted along dimension to compute the neighbourhoods for.
   * @param dimension The dimension along which to compute the neighbourhoods.
   * @param epsilon The search radius.
   * @return The neighbourhoods (inner arrays) of each data point (outer array) as indices
   *         of the respective data points in the input array (Int), ordered the same way as the input.
   * @note Data points must be sorted along dimension before passing them to this function!
   */
  def apply(points: Array[DataPoint], dimension: Int, epsilon: Float, minPts: Int): Array[Array[Int]] = {
    execute(points, dimension, epsilon, minPts)
  }

  def execute(points: Array[DataPoint], dimension: Int, epsilon: Float, minPts: Int): Array[Array[Int]] = {
    val neighbourhoods: Array[TinyArrayBuffer] = Array.fill(points.length)(new TinyArrayBuffer(32))
    val srLowerBound: Array[Float] = new Array[Float](points.head.dimensions)
    val srUpperBound: Array[Float] = new Array[Float](points.head.dimensions)

    for (l <- points.indices) {
      val lPoint = points(l)

      for (i <- lPoint.vectorRep.indices) {
        srLowerBound(i) = lPoint.vectorRep(i) - epsilon
        srUpperBound(i) = lPoint.vectorRep(i) + epsilon
      }

      var u = l + 1
      while (u < points.length && points(u).vectorRep(dimension) - lPoint.vectorRep(dimension) <= epsilon) {
        val uPoint = points(u)
        if (inSearchRegion(srLowerBound, srUpperBound, uPoint) && lPoint.distance(uPoint) <= epsilon) {
          neighbourhoods(l) += u
          neighbourhoods(u) += l
        }
        u += 1
      }

      // If lPoint has < minPts - 1 neighbours, we can delete its neighbourhood to save memory
      if (neighbourhoods(l).length < minPts - 1) neighbourhoods(l) = new TinyArrayBuffer()
    }
    neighbourhoods.map(_.toArray)
  }

  final def inSearchRegion(srLowerBound: Array[Float], srUpperBound: Array[Float], point: DataPoint): Boolean = {
    var i = 0
    val n = point.dimensions
    while (i < n) {
      if (point.vectorRep(i) < srLowerBound(i) || point.vectorRep(i) > srUpperBound(i)) {
        return false
      }
      i += 1
    }
    true
  }
}
