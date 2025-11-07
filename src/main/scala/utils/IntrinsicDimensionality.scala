package utils

import model.DataPoint

object IntrinsicDimensionality {
  /**
   * Computes the estimated intrinsic dimensionality of a given sample of the dataset.
   *
   * @param sampleDataset    The dataset consisting of an array of DataPoint objects.
   * @return The estimated intrinsic dimensionality of the dataset.
   */
  def apply(sampleDataset: Array[DataPoint]): Double = {
    execute(sampleDataset)
  }

  final def execute(sampleDataset: Array[DataPoint]): Double = {
    val distances: Array[Double] = new Array[Double]((sampleDataset.length * (sampleDataset.length - 1))/ 2)
    var pointer = 0
    for (i <- sampleDataset.indices) {
      for (j <- i + 1 until sampleDataset.length) {
        distances(pointer) = sampleDataset(i).distance(sampleDataset(j))
        pointer += 1
      }
    }
    val mean: Double = distances.sum / distances.length

    var sum: Double = 0.0
    for (d <- distances) {
      val diff = d - mean
      sum += diff * diff
    }
    val variance: Double = sum / distances.length

    Math.pow(mean, 2) / (2 * variance)
  }
}
