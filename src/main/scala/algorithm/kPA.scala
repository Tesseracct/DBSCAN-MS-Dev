package algorithm

import model.{DataPoint, MASK, Subspace}
import utils.MapPointToVectorSpace

object kPA {
  /**
   * Computes the partitions for a given point using the kPA algorithm.
   * @param point The point to partition.
   * @param pivots The pivots used for mapping the point to the vector space.
   * @param subspaces The subspaces to partition the point into.
   * @return A list of tuples containing the point and the partition index.
   */
  def apply(point: DataPoint, pivots: Array[DataPoint], subspaces: Array[Subspace]): List[(Int, DataPoint)] = {
    execute(point, pivots, subspaces)
  }

  def execute(point: DataPoint, pivots: Array[DataPoint], subspaces: Array[Subspace]): List[(Int, DataPoint)] = {
    val newPoint: DataPoint = if (point.vectorRep == null) point.withVectorRep(MapPointToVectorSpace(point, pivots)) else point

    var returnList = List[(Int, DataPoint)]()
    for (i <- subspaces.indices) {
      val subspace = subspaces(i)
      if (inside(newPoint, subspace.outer)) {
        val mask = (inside(newPoint, subspace.bbCoords), inside(newPoint, subspace.inner)) match {
          case (true, true)  => MASK.SPACE_INNER
          case (true, false) => MASK.MARGIN_INNER
          case (false, _)    => MASK.MARGIN_OUTER
        }
        val pointWithMask = newPoint.withMask(mask)
        pointWithMask.partition = i
        returnList = returnList :+ (i, pointWithMask)
      }
    }

    returnList
  }


  /**
   * Checks if a point is inside a subspace.
   * @param point The point to check.
   * @param subspaceCoords The bounding box of the subspace.
   * @return True if the point is inside the subspace, false otherwise.
   */
  def inside(point: DataPoint, subspaceCoords: Array[(Float, Float)]): Boolean = {
    require(point.vectorRep.length == subspaceCoords.length, "Point and subspace must have the same dimension")
    point.vectorRep.zip(subspaceCoords).forall(x => (x._2._1.isNaN || x._1 >= x._2._1) && (x._2._2.isNaN || x._1 <= x._2._2))
  }
}
