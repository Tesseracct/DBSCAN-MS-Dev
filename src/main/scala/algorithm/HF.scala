package algorithm

import model.DataPoint

import scala.util.Random

object HF {
  /** Selects pivot candidates using the Hull Foci (HF) algorithm.
   *
   * @param dataset The sampled dataset from which to select pivot candidates.
   * @param numberOfPivotCandidates The number of pivot candidates to select.
   * @param distanceFunction The distance function to use for distance calculations.
   * @param seed Random seed for reproducibility.
   * @return An array of selected pivot candidates.
   */
  def apply(dataset: Array[DataPoint],
            numberOfPivotCandidates: Int = 40,
            distanceFunction: (Array[Float], Array[Float]) => Float,
            seed: Int): Array[DataPoint] = {
    execute(dataset, numberOfPivotCandidates, distanceFunction, seed)
  }

  def execute(dataset: Array[DataPoint],
                            numberOfPivotCandidates: Int,
                            distanceFunction: (Array[Float], Array[Float]) => Float,
                            seed: Int): Array[DataPoint] = {
    require(dataset.length > numberOfPivotCandidates, "Number of pivot candidates must be smaller than the dataset size!")

    val rng = new Random(seed)
    val pivotCandidates = new Array[DataPoint](numberOfPivotCandidates)
    val startingPoint = dataset(rng.nextInt(dataset.length))

    pivotCandidates(0) = findFarthestPoint(dataset, startingPoint, distanceFunction)
    pivotCandidates(1) = findFarthestPoint(dataset, pivotCandidates(0), distanceFunction)

    val edge = pivotCandidates(0).distance(pivotCandidates(1), distanceFunction)

    val errors = new Array[Float](dataset.length)
    for (i <- dataset.indices if !pivotCandidates.contains(dataset(i))) {
      errors(i) = Math.abs(edge - dataset(i).distance(pivotCandidates(0), distanceFunction))
    }

    for (i <- 2 until numberOfPivotCandidates) {
      var minimalError = Float.MaxValue
      var bestCandidate: DataPoint = null
      for (j <- dataset.indices if !pivotCandidates.contains(dataset(j))) {
        val error = errors(j) + Math.abs(edge - dataset(j).distance(pivotCandidates(i - 1), distanceFunction))
        errors(j) = error
        if (error < minimalError) {
          minimalError = error
          bestCandidate = dataset(j)
        }
      }
      pivotCandidates(i) = bestCandidate
    }
    pivotCandidates
  }

  /** Finds the point in the dataset that is farthest from the reference point.
   *
   * @param dataset The dataset to search.
   * @param referencePoint The reference point.
   * @param distanceFunction The distance function to use for distance calculations.
   * @return The farthest point from the reference point in the dataset.
   */
  private[algorithm] def findFarthestPoint(dataset: Array[DataPoint],
                                           referencePoint: DataPoint,
                                           distanceFunction: (Array[Float], Array[Float]) => Float): DataPoint = {

    var maxDistance = Float.MinValue
    var farthestPoint: DataPoint = null

    for (point <- dataset if point != referencePoint) {
      val distance = referencePoint.distance(point, distanceFunction)
      if (distance > maxDistance) {
        maxDistance = distance
        farthestPoint = point
      }
    }

    require(farthestPoint != null, "No valid farthest point found")
    farthestPoint
  }

}
