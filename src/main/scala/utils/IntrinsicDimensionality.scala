package utils

import model.DataPoint

object IntrinsicDimensionality {
  /**
   * Computes the estimated intrinsic dimensionality of a given sample of the dataset.
   *
   * @param sampleDataset    The dataset consisting of an array of DataPoint objects.
   * @return The estimated intrinsic dimensionality of the dataset.
   */
  def apply[A](sampleDataset: Array[DataPoint[A]])(implicit m: Metric[A]): Double = {
    execute(sampleDataset)
  }

  final def execute[A](sampleDataset: Array[DataPoint[A]])(implicit m: Metric[A]): Double = {
    val n = sampleDataset.length

    var k = 0L
    var mean = 0.0
    var m2 = 0.0

    for (i <- sampleDataset.indices) {
      for (j <- i + 1 until n) {
        val x = sampleDataset(i).distance(sampleDataset(j))
        k += 1
        val delta = x - mean
        mean += delta / k
        val delta2 = x - mean
        m2 += delta * delta2
      }
    }

    val variance: Double = m2 / (k - 1).toDouble
    Math.pow(mean, 2) / (2 * variance)
  }
}
