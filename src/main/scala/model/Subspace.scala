package model

/**
 * Represents a subspace defined by a set of data points and their bounding box coordinates.
 *
 * @param points   The data points contained within the subspace.
 * @param bbCoords The bounding box coordinates for each dimension, represented as tuples of (min, max).
 */
class Subspace(val points: Array[DataPoint], val bbCoords: Array[(Float, Float)]) {

  /**
   * Splits the subspace into two subspaces along the specified dimension at the median value.
   *
   * @param dimension The dimension along which to split the subspace.
   * @return A tuple containing the two resulting subspaces.
   */
  def split(dimension: Int): (Subspace, Subspace) = {
    // TODO: Sorting can be avoided by using Quickselect for average-case O(n)
    val median = points.map(_.vectorRep(dimension)).sorted.apply(points.length / 2) // Choosing the floor here, as per k-d tree
    val (leftPoints, rightPoints) = points.partition(_.vectorRep(dimension) <= median)
    val leftBB = bbCoords.updated(dimension, (bbCoords(dimension)._1, median))
    val rightBB = bbCoords.updated(dimension, (median, bbCoords(dimension)._2))
    (new Subspace(leftPoints, leftBB), new Subspace(rightPoints, rightBB))
  }

  def outerSubspace(epsilon: Float): Subspace = {
    val newBB = bbCoords.map { case (min, max) => (min - epsilon, max + epsilon) }
    new Subspace(points, newBB)
  }

}
