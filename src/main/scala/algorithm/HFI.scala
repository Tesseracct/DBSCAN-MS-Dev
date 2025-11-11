package algorithm

import model.DataPoint
import utils.MapPointToVectorSpace

import scala.collection.parallel.CollectionConverters.IterableIsParallelizable
import scala.util.Random


object HFI {
  private val log = org.apache.log4j.LogManager.getLogger(HFI.getClass)
  /**
   * Selects pivots using the Hull Foci Algorithm (HFI).
   *
   * @param dataset The sampled dataset from which to select pivots.
   * @param numberOfPivots The number of pivots to select.
   * @param seed Random seed for reproducibility.
   * @return An array of selected pivots.
   */
  def apply(dataset: Array[DataPoint],
            numberOfPivots: Int,
            seed: Int = Random.nextInt()): Array[DataPoint] = {
    execute(dataset, numberOfPivots, seed)
  }

  def execute(dataset: Array[DataPoint],
              numberOfPivots: Int,
              seed: Int = Random.nextInt()): Array[DataPoint] = {
    require(dataset.nonEmpty, "Dataset must not be empty")
    require(numberOfPivots >= 2, "Number of pivots must be at least 2")
    require(numberOfPivots <= dataset.length, "Number of pivots must not exceed dataset size")
    if (dataset.length >= 4500) log.warn(s"Warning in $this! Sampled dataset has ${dataset.length} elements. " +
      s"Pivot selection for large datasets is expensive because of quadratic complexity.")

    // Standard count of pivot candidates set to 40 as per Efficient Metric Indexing for Similarity Search, Section III B.
    val numberOfPivotCandidates = if (numberOfPivots > 30) numberOfPivots * 2 else 40
    val candidates = HF(dataset, numberOfPivotCandidates, seed)
    val distanceMatrix = computeDistanceMatrix(dataset)

    val pivots = new Array[DataPoint](numberOfPivots)
    for (i <- 0 until numberOfPivots) {
      def precisionWithTrial(trial: DataPoint): Float = {
        val pivCopy = pivots.clone()
        pivCopy(i) = trial
        newPivotSetPrecision(dataset, distanceMatrix, pivCopy, i)
      }

      val (maxPrecision, bestCandidate, bestCandidateIndex) =
        candidates.indices.view.filter(candidates(_) != null)
                                .par
                                .map { j =>
                                  val c = candidates(j)
                                  (precisionWithTrial(c), c, j) }
                                .maxBy(_._1)

      if (bestCandidate != null) {
        pivots(i) = bestCandidate
        candidates(bestCandidateIndex) = null
      } else {
        throw new RuntimeException("No valid pivot candidate found")
      }
    }

    pivots
  }

  /**
   * Computes the precision of the pivot set with the new pivot candidate.
   *
   * (This is precision(P) in "Efficient Metric Indexing for Similarity Search")
   * @param dataset The sampled dataset used for evaluating the pivot selection.
   * @param pivots The pivots used for mapping the data points to the vector space. The last pivot is the new pivot candidate.
   * @return The average precision of the pivot selection.
   */
  private[algorithm] def newPivotSetPrecision(dataset: Array[DataPoint],
                                              distanceMatrix: Array[Array[Float]],
                                              pivots: Array[DataPoint],
                                              pointer: Int): Float = {
    val opCardinality = dataset.length * (dataset.length - 1) / 2.0f
    val mappedDataset = dataset.map(MapPointToVectorSpace(_, pivots, pointer))
    var sum = 0.0f
    for (i <- dataset.indices) {
      for (j <- i + 1 until dataset.length) {
        sum += L_infNorm(mappedDataset(i), mappedDataset(j)) / distanceMatrix(i)(j - i - 1)
      }
    }
    sum / opCardinality
  }


  // TODO: Consider sampled object pairs for large datasets.
  private[algorithm] def DEPR_newPivotSetPrecision(objectPairs: Array[(DataPoint, DataPoint)], pivots: Array[DataPoint]): Float = {
    objectPairs.map { case (a, b) =>
      L_infNorm(MapPointToVectorSpace(a, pivots), MapPointToVectorSpace(b, pivots)) / a.distance(b)
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
  final def L_infNorm(a: Array[Float], b: Array[Float]): Float = {
    require(a.length == b.length, "Data points must have the same dimension")
    var i = 0
    var max = 0.0f
    val n = a.length
    while (i < n) {
      val x = Math.abs(a(i) - b(i))
      if (x > max) max = x
      i += 1
    }
    max
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
    if(dataset.length > sampleSize) log.warn(s"Warning in $this! Dataset should be larger than sample size!") // Not strictly necessary but guards against weird cases
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

  /**
   * Computes the pairwise distance matrix for a given dataset using a specified distance function.
   *
   * @param dataset The input array of DataPoint objects for which the pairwise distances will be computed.
   * @return The upper triangle of the pairwise distance matrix.
   */
  def computeDistanceMatrix(dataset: Array[DataPoint]): Array[Array[Float]] = {
    val distanceMatrix: Array[Array[Float]] = new Array[Array[Float]](dataset.length)
    for (i <- dataset.indices) {
      distanceMatrix(i) = new Array[Float](dataset.length - i - 1)
    }
    for (i <- dataset.indices) {
      for (j <- i + 1 until dataset.length) {
        distanceMatrix(i)(j - i - 1) = dataset(i).distance(dataset(j))
      }
    }
    distanceMatrix
  }
}
