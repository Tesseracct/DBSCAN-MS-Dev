package algorithm

import model.DataPoint
import utils.Distance.euclidean

import scala.collection.mutable.ArrayBuffer


case object SWNQA {
  /**
   * Computes the neighbourhoods of a given set of data points.
   * @param points The data points sorted along dimension to compute the neighbourhoods for.
   * @param dimension The dimension along which to compute the neighbourhoods.
   * @param epsilon The search radius.
   * @return The neighbourhoods (inner arrays) of each data point (outer array) as indices
   *         of the respective data points in the input array (Int), ordered the same way as the input.
   * @note Data points must be sorted along dimension before passing them to this function!
   */
  def apply(points: Array[DataPoint], dimension: Int, epsilon: Float): Array[Array[Int]] = {
    val neighbourhoods: Array[ArrayBuffer[Int]] = Array.fill(points.length)(ArrayBuffer[Int]())

    for (l <- points.indices) {
      val lPoint = points(l)
      val searchRegion = lPoint.vectorRep.map(x => (x - epsilon, x + epsilon))

      var u = l
      while (u < points.length && points(u).vectorRep(dimension) - lPoint.vectorRep(dimension) <= epsilon) {
        val uPoint = points(u)
        if (inSearchRegion(searchRegion, uPoint) && lPoint.distance(uPoint, euclidean) <= epsilon) {
          neighbourhoods(l) += u
          neighbourhoods(u) += l
        }
        u = u + 1
      }
    }
    neighbourhoods.map(_.toArray)
  }

  def inSearchRegion(searchRegion: Array[(Float, Float)], point: DataPoint): Boolean = {
    for (i <- searchRegion.indices) {
      if (point.vectorRep(i) < searchRegion(i)._1 || point.vectorRep(i) > searchRegion(i)._2) {
        return false
      }
    }
    true
  }
}
