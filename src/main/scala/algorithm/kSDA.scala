package algorithm

import model.DataPoint
import utils.MapPointToVectorSpace

import scala.util.Random
import scala.collection.mutable

// k-d tree based Space Dividing Algorithm (kSDA)
object kSDA {
  /**
   * Divides the given dataset into a specified number of partitions using a k-d tree approach.
   * Each partition is represented by a bounding-box.
   *
   * @param dataset The dataset to be divided, represented as an array of DataPoint.
   * @param numberOfPartitions The desired number of partitions.
   * @param seed Optional seed for reproducibility. Defaults to a random seed.
   * @return An array of coordinates as tuples, in the form of (min, max).
   */
  def divideSpace(dataset: Array[DataPoint], pivots: Array[DataPoint], numberOfPartitions: Int, seed: Int = Random.nextInt()): Array[(Array[Float], Array[Float])] = {
    require(dataset.nonEmpty, "Dataset must not be empty")
    require(numberOfPartitions > 0, "Number of partitions must be greater than zero")

    val rng = new Random(seed)

    dataset.foreach(point => point.vectorRep = MapPointToVectorSpace(point, pivots))
    val q = mutable.Queue.apply(dataset)

    while(q.length < numberOfPartitions) {
      val currentPartition = q.dequeue()
      val randomDimension = rng.nextInt(currentPartition.head.vectorRep.length)

      val median = currentPartition.map(_.vectorRep(randomDimension)).sorted.apply(currentPartition.length / 2) // Choosing the floor here, as per k-d tree
      val (a, b) = currentPartition.partition(_.vectorRep(randomDimension) <= median)
      if (a.nonEmpty) q.enqueue(a)
      if (b.nonEmpty) q.enqueue(b)
    }

    // Compute bounding boxes for each partition by finding min and max in each dimension
    q.toArray.map(partition => {
      val coords = partition.map(_.vectorRep).transpose // TODO: How expensive is this?
      (coords.map(_.min), coords.map(_.max))
    })
  }
}
