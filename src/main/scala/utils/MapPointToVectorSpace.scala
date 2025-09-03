package utils

import model.DataPoint
import utils.Distance.euclidean

object MapPointToVectorSpace {
  /**
   * Maps a data point to a vector space defined by the given pivots.
   *
   * (This is ϕ(o) in "Efficient Metric Indexing for Similarity Search")
   * @param point The data point to map.
   * @param pivots The pivots defining the vector space.
   * @return The coordinates of the data point in the vector space.
   */
  def apply(point: DataPoint, pivots: Array[DataPoint]): Array[Float] = {
    pivots.map(pivot => pivot.distance(point, euclidean))
  }

  def apply(point: DataPoint, pivots: List[DataPoint]): List[Float] = {
    pivots.map(pivot => pivot.distance(point, euclidean))
  }

}
