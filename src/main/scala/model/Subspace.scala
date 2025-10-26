package model

import utils.Quickselect

/**
 * Represents a subspace defined by a set of data points and their bounding box coordinates.
 *
 * @param points   The data points contained within the subspace.
 * @param bbCoords The bounding box coordinates for each dimension, represented as tuples of (min, max).
 * @param epsilon  The search radius.
 */
case class Subspace (points: Array[DataPoint], bbCoords: Array[(Float, Float)], epsilon: Float) {
  val outer: Array[(Float, Float)] = this.outerSubspace(epsilon)
  val inner: Array[(Float, Float)] = this.innerSubspace(epsilon)

  /**
   * Splits the subspace into two subspaces along the specified dimension at the median value.
   *
   * @param dimension The dimension along which to split the subspace.
   * @return A tuple containing the two resulting subspaces.
   */
  def split(dimension: Int): (Subspace, Subspace) = {
    val median = Quickselect.select(points.map(_.vectorRep(dimension)), points.length / 2)
    val (leftPoints, rightPoints) = points.partition(_.vectorRep(dimension) < median)
    val leftBB = bbCoords.updated(dimension, (bbCoords(dimension)._1, median))
    val rightBB = bbCoords.updated(dimension, (median, bbCoords(dimension)._2))
    (Subspace(leftPoints, leftBB, epsilon), Subspace(rightPoints, rightBB, epsilon))
  }

  /**
   * Computes the ε-Inner Subspace of the Subspace by adjusting the bounding box
   * coordinates outward according to Definition 8.
   *
   * @param epsilon The search radius.
   * @return An array of tuples representing the outer subspace.
   */
  private def outerSubspace(epsilon: Float): Array[(Float, Float)] = {
    bbCoords.map { case (min, max) => (min - epsilon, max + epsilon) }
  }

  /**
   * Computes the ε-Inner Subspace of the Subspace by adjusting the bounding box
   * coordinates inward according to Definition 8.
   *
   * @param epsilon The search radius.
   * @return An array of tuples representing the inner subspace.
   */
  private def innerSubspace(epsilon: Float): Array[(Float, Float)] = {
    bbCoords.map { case (min, max) => (min + epsilon, max - epsilon) }
  }


}
