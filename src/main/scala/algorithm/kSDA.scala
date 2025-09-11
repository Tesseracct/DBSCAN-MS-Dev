package algorithm

import model.{DataPoint, Subspace}
import utils.MapPointToVectorSpace

import scala.util.Random
import scala.collection.mutable

// k-d tree based Space Dividing Algorithm (kSDA)
object kSDA {
  /**
   * Divides the given dataset into a specified number of partitions using the k-d tree approach.
   * Each partition is represented by a bounding-box.
   *
   * @param dataset The sampled dataset to be divided, represented as an array of DataPoint.
   * @param numberOfPartitions The desired number of partitions.
   * @param seed Optional seed for reproducibility. Defaults to a random seed.
   * @return An array of Subspaces containing the coordinates of the bounding box.
   */
  def apply(dataset: Array[DataPoint], pivots: Array[DataPoint], numberOfPartitions: Int, seed: Int = Random.nextInt(), epsilon: Float): Array[Subspace] = {
    require(dataset.nonEmpty, "Dataset must not be empty")
    require(numberOfPartitions > 0, "Number of partitions must be greater than zero")

    val rng = new Random(seed)

    dataset.foreach(point => point.vectorRep = MapPointToVectorSpace(point, pivots))
    val subspace: Subspace = new Subspace(dataset, Array.fill(dataset.head.dimensions)((Float.NaN, Float.NaN)), epsilon)
    val q = mutable.Queue.apply(subspace)

    while (q.length < numberOfPartitions) {
      val currentPartition = q.dequeue()
      val randomDimension = rng.nextInt(currentPartition.points.head.dimensions)
      val (a, b) = currentPartition.split(randomDimension)
      if (a.points.nonEmpty) q.enqueue(a) // TODO: Might be too expensive with little gain
      if (b.points.nonEmpty) q.enqueue(b)
    }

    q.toArray
  }
}
