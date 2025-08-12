package utils

import model.DataPoint

case object VectorToMetric {
  /**
   * Converts an array of DataPoint objects into a metric space by calculating pairwise distances.
   * Each DataPoint's distances array is populated with the Euclidean distance to every other DataPoint.
   *
   * @param dataset The dataset to be converted, represented as an array of DataPoint.
   */
  def apply(dataset: Array[DataPoint]): Unit = {
    require(dataset.nonEmpty, "Vectors must not be empty")
    require(dataset.forall(_.coordinates.nonEmpty), "All vectors must be non-empty")
    require(dataset.forall(_.coordinates.length == dataset.head.coordinates.length), "All vectors must have the same length")

    dataset.foreach(_.distances = new Array[Float](dataset.length))
    for (i <- dataset.indices) {
      require(dataset(i).id == i, "DataPoint IDs must match their indices in the dataset")

      for (j <- i until dataset.length) {
        val distance = Distance.euclidean(dataset(i).coordinates, dataset(j).coordinates)
        dataset(i).distances(j) = distance
        dataset(j).distances(i) = distance
      }
    }
  }
}
