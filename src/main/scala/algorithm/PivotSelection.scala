package algorithm

import model.DataPoint
import utils.Distance.euclidean

import scala.util.Random

case object PivotSelection {
  // Hull Foci Algorithm to find pivot candidates
  def HF(dataset: Array[DataPoint],
         numberOfPivotCandidates: Int,
         distanceFunction: (Array[Float], Array[Float]) => Float,
         seed: Int): Array[DataPoint] = {

    val rng = new Random(seed)
    val pivotCandidates = new Array[DataPoint](numberOfPivotCandidates)
    val startingPoint = dataset(rng.nextInt(dataset.length))

    pivotCandidates(0) = findFarthestPoint(dataset, startingPoint, distanceFunction)
    pivotCandidates(1) = findFarthestPoint(dataset, pivotCandidates(0), distanceFunction)

    val edge = distanceFunction(pivotCandidates(0).coordinates, pivotCandidates(1).coordinates)

    // TODO: This is inefficient because we're computing distances to pivot candidates multiple times.
    for (i <- 2 until numberOfPivotCandidates) {
      var minimalError = Float.MaxValue
      var bestCandidate: DataPoint = null

      for (point <- dataset if !pivotCandidates.contains(point)) {
        var error = 0.0f
        for (pivot <- pivotCandidates.take(i)) {
          error += Math.abs(edge - distanceFunction(point.coordinates, pivot.coordinates))
        }

        if (error < minimalError) {
          minimalError = error
          bestCandidate = point
        }
      }

      pivotCandidates(i) = bestCandidate
    }


    /*
    pivotCandidates.indices.drop(2).foreach(i => {
      dataset.filter(point => !pivotCandidates.contains(point)).map(point => {
        (point, pivotCandidates.take(i).map(candidate => {
          Math.abs(edge - distanceFunction(point.coordinates, candidate.coordinates))
        }).sum)
      }).minBy(_._2) match {
        case (bestCandidate, _) => pivotCandidates(i) = bestCandidate
      }
    })
    */

    pivotCandidates
  }

  def findFarthestPoint(dataset: Array[DataPoint],
                                referencePoint: DataPoint,
                                distanceFunction: (Array[Float], Array[Float]) => Float): DataPoint = {

    var maxDistance = Float.MinValue
    var farthestPoint: DataPoint = null

    for (point <- dataset if point != referencePoint) {
        val distance = distanceFunction(referencePoint.coordinates, point.coordinates)
        if (distance > maxDistance) {
          maxDistance = distance
          farthestPoint = point
      }
    }

    require(farthestPoint != null, "No valid farthest point found")
    farthestPoint
  }

  // HF based Incremental Pivot Selection Algorithm
  def HFI(dataset: Array[DataPoint],
          numberOfPivots: Int = 40,
          distanceFunction: (Array[Float], Array[Float]) => Float = euclidean,
          seed: Int = Random.nextInt()): Unit = {
    require(dataset.nonEmpty, "Dataset must not be empty")
    require(numberOfPivots >= 2, "Number of pivots must be at least 2")
    require(numberOfPivots <= dataset.length, "Number of pivots must not exceed dataset size")

    val pivotCandidates = HF(dataset, numberOfPivots, distanceFunction, seed)


  }
}
