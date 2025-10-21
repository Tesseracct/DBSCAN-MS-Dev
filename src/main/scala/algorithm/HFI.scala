package algorithm

import model.DataPoint
import utils.Distance.euclidean
import utils.MapPointToVectorSpace

import scala.util.Random


object HFI {
  /**
   * Selects pivots using the Hull Foci Algorithm (HFI).
   *
   * @param dataset The sampled dataset from which to select pivots.
   * @param numberOfPivots The number of pivots to select.
   * @param distanceFunction The distance function to use for distance calculations.
   * @param seed Random seed for reproducibility.
   * @return An array of selected pivots.
   */
  def apply(dataset: Array[DataPoint],
            numberOfPivots: Int,
            distanceFunction: (Array[Float], Array[Float]) => Float = euclidean,
            seed: Int = Random.nextInt()): Array[DataPoint] = {
    execute(dataset, numberOfPivots, distanceFunction, seed)
  }

  def execute(dataset: Array[DataPoint],
            numberOfPivots: Int,
            distanceFunction: (Array[Float], Array[Float]) => Float = euclidean,
            seed: Int = Random.nextInt()): Array[DataPoint] = {
    require(dataset.nonEmpty, "Dataset must not be empty")
    require(numberOfPivots >= 2, "Number of pivots must be at least 2")
    require(numberOfPivots <= dataset.length, "Number of pivots must not exceed dataset size")
    if (dataset.length >= 4500) println(s"Warning in $this! Sampled dataset has ${dataset.length} elements. " +
      s"Pivot selection for large datasets is expensive because of quadratic complexity.")

    // Standard count of pivot candidates set to 40 as per Efficient Metric Indexing for Similarity Search Section III B.
    val numberOfPivotCandidates = if (numberOfPivots > 30) numberOfPivots * 2 else 40
    val candidates = HF(dataset, numberOfPivotCandidates, distanceFunction, seed)
    var pivots = List[DataPoint]()

    for (_ <- 0 until numberOfPivots) {
      var maxPrecision = Float.MinValue
      var bestCandidate: DataPoint = null
      var bestCandidateIndex = -1

      for (j <- candidates.indices) {
        if (candidates(j) != null) {
          pivots = candidates(j) :: pivots

          val newPrecision = newPivotSetPrecision(dataset, pivots)
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
   * @param dataset The sampled dataset used for evaluating the pivot selection.
   * @param pivots The pivots used for mapping the data points to the vector space. The last pivot is the new pivot candidate.
   * @return The average precision of the pivot selection.
   */
  private[algorithm] def newPivotSetPrecision(dataset: Array[DataPoint], pivots: List[DataPoint]): Float = {
    val opCardinality = dataset.length * (dataset.length - 1) / 2.0f
    val mappedDataset = dataset.map(MapPointToVectorSpace(_, pivots))
    var sum = 0.0f
    for (i <- dataset.indices) {
      for (j <- i + 1 until dataset.length) {
        sum += L_infNorm(mappedDataset(i), mappedDataset(j)) / dataset(i).distance(dataset(j), euclidean)
      }
    }
    sum / opCardinality
  }


  // TODO: Consider sampled object pairs for large datasets.
  private[algorithm] def DEPR_newPivotSetPrecision(objectPairs: Array[(DataPoint, DataPoint)], pivots: List[DataPoint]): Float = {
    objectPairs.map { case (a, b) =>
      L_infNorm(MapPointToVectorSpace(a, pivots), MapPointToVectorSpace(b, pivots)) / a.distance(b, euclidean)
    }.sum / objectPairs.length
  }

  /**
   * Computes the L-infinity norm (Chebyshev distance) between two data points.
   *
   * (This is D() in "Efficient Metric Indexing for Similarity Search")
   * @param a First coordinates.
   * @param b Second coordinates.
   * @return The L-infinity norm between the two data points.
   */
  def L_infNorm(a: List[Float], b: List[Float]): Float = {
    require(a.length == b.length, "Data points must have the same dimension")
    a.zip(b).map { case (x, y) => Math.abs(x - y) }.max
  }

  /**
   * Samples unique pairs of data points from the dataset.
   *
   * @param dataset The dataset from which to sample pairs.
   * @param sampleSize The number of unique pairs to sample.
   * @param seed Random seed for reproducibility.
   * @return An array of unique pairs of data points.
   */
  def samplePairs(dataset: Array[DataPoint], sampleSize: Int, seed: Int): Array[(DataPoint, DataPoint)] = {
    if(dataset.length > sampleSize) println(s"Warning in $this! Dataset should be larger than sample size!") // Not strictly necessary but guards against weird cases
    val rng = new Random(seed)
    var pairs = Set[(DataPoint, DataPoint)]()

    while (pairs.size < sampleSize) {
      val a = dataset(rng.nextInt(dataset.length))
      val b = dataset(rng.nextInt(dataset.length))
      if (a != b) {
        val pair = if (a.id < b.id) (a, b) else (b, a)
        pairs += pair
      }
    }
    pairs.toArray
  }

}
