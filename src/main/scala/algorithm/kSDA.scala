package algorithm

import model.DataPoint

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
  def divideSpace(dataset: Array[DataPoint], numberOfPartitions: Int, seed: Int = Random.nextInt()): Array[(Array[Float], Array[Float])] = {
    require(dataset.nonEmpty, "Dataset must not be empty")
    require(numberOfPartitions > 0, "Number of partitions must be greater than zero")

    val rng = new Random(seed)
    val q = mutable.Queue.apply(dataset)

    while(q.length < numberOfPartitions) {
      val currentPartition = q.dequeue()
      val randomDimension = rng.nextInt(currentPartition.head.coordinates.length)

      // TODO: Look at better ways to select median (Quickselect?)
      val median = currentPartition.map(_.coordinates(randomDimension)).sorted.apply(currentPartition.length / 2) // Choosing the floor here, as per k-d tree
      val (a, b) = currentPartition.partition(_.coordinates(randomDimension) <= median)
      if (a.nonEmpty) q.enqueue(a)
      if (b.nonEmpty) q.enqueue(b)
    }

    q.toArray.map(x => {
      val coords = x.map(_.coordinates).transpose // TODO: How expensive is this?
      (coords.map(_.min), coords.map(_.max))
    })
  }
}
