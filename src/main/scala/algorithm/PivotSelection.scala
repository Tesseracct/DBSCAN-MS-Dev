package algorithm

import model.{DataPoint, DataPointVector}
import utils.Distance.euclidean
import utils.MapPointToVectorSpace

import scala.util.Random


case object PivotSelection {
  // Hull Foci Algorithm to find pivot candidates
  private[algorithm] def HF(dataset: Array[DataPoint],
                            numberOfPivotCandidates: Int,
                            distanceFunction: (Array[Float], Array[Float]) => Float,
                            seed: Int): Array[DataPoint] = {

    val rng = new Random(seed)
    val pivotCandidates = new Array[DataPoint](numberOfPivotCandidates)
    val startingPoint = dataset(rng.nextInt(dataset.length))

    pivotCandidates(0) = findFarthestPoint(dataset, startingPoint, distanceFunction)
    pivotCandidates(1) = findFarthestPoint(dataset, pivotCandidates(0), distanceFunction)

    val edge = pivotCandidates(0).distance(pivotCandidates(1), distanceFunction)

    // TODO: This is inefficient because we're computing distances to pivot candidates multiple times.
    for (i <- 2 until numberOfPivotCandidates) {
      var minimalError = Float.MaxValue
      var bestCandidate: DataPoint = null

      for (point <- dataset if !pivotCandidates.contains(point)) {
        var error = 0.0f
        for (pivot <- pivotCandidates.take(i)) {
          error += Math.abs(edge - point.distance(pivot, distanceFunction))
        }

        if (error < minimalError) {
          minimalError = error
          bestCandidate = point
        }
      }

      pivotCandidates(i) = bestCandidate
    }

    pivotCandidates
  }

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

  /**
   * Selects pivots using the Hull Foci Algorithm (HFI).
   *
   * @param dataset The dataset from which to select pivots.
   * @param numberOfPivots The number of pivots to select.
   * @param distanceFunction The distance function to use for distance calculations.
   * @param seed Random seed for reproducibility.
   */
  def HFI(dataset: Array[DataPoint],
          numberOfPivots: Int = 40,
          distanceFunction: (Array[Float], Array[Float]) => Float = euclidean,
          seed: Int = Random.nextInt()): Array[DataPoint] = {
    require(dataset.nonEmpty, "Dataset must not be empty")
    require(numberOfPivots >= 2, "Number of pivots must be at least 2")
    require(numberOfPivots <= dataset.length, "Number of pivots must not exceed dataset size")

    val candidates = HF(dataset, numberOfPivots, distanceFunction, seed)
    val objectPairs = null // TODO: Choose object pairs
    var pivots = List[DataPoint]()

    for (_ <- 0 until numberOfPivots) {
      var maxPrecision = Float.MinValue
      var bestCandidate: DataPoint = null
      var bestCandidateIndex = -1

      for (j <- candidates.indices) {
        if (candidates(j) != null) {
          pivots = candidates(j) :: pivots

          val newPrecision = newPivotSetPrecision(objectPairs, pivots)
          if (newPrecision > maxPrecision) {
            maxPrecision = newPrecision
            bestCandidate = candidates(j)
            bestCandidateIndex = j
          }

          pivots = pivots.tail
        }
      }

      if (bestCandidate != null) {
        pivots = bestCandidate :: pivots
        candidates(bestCandidateIndex) = null
      } else {
        throw new RuntimeException("No valid pivot candidate found")
      }
    }

    pivots.toArray
  }

  /**
   * Computes the precision of the pivot set with the new pivot candidate.
   *
   * (This is precision(P) in "Efficient Metric Indexing for Similarity Search")
   * @param objectPairs An array of pairs of data points.
   * @param pivots The pivots used for mapping the data points to the vector space. The last pivot is the new pivot candidate.
   * @return The average precision of the pivot selection.
   */
  private[algorithm] def newPivotSetPrecision(objectPairs: Array[(DataPoint, DataPoint)], pivots: List[DataPoint]): Float = {
    objectPairs.map { case (a, b) =>
      L_infNorm(MapPointToVectorSpace(a, pivots), MapPointToVectorSpace(b, pivots)) / a.distance(b, euclidean)
    }.sum / objectPairs.length
  }

  /**
   * Computes the L-infinity norm (Chebyshev distance) between two data points.
   *
   * (This is D() in "Efficient Metric Indexing for Similarity Search")
   * @param a First data point.
   * @param b Second data point.
   * @return The L-infinity norm between the two data points.
   */
  private def L_infNorm(a: List[Float], b: List[Float]): Float = {
    require(a.length == b.length, "Data points must have the same dimension")
//    a.data.zip(b.data).map { case (x, y) => Math.abs(x - y) }.max

    var maxDiff = Float.MinValue
    for (i <- a.indices) {
      val diff = Math.abs(a(i) - b(i))
      if (diff > maxDiff) {
        maxDiff = diff
      }
    }
    maxDiff
  }


}
