package algorithm

import model.DataPoint
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class kSDATest extends AnyFunSuite {

  test("kSDA.divideSpace returns correct number of partitions") {
    val dataset = (1 to 1000).map { _ =>
      DataPoint(Array(Random.nextFloat(), Random.nextFloat()), 0)
    }.toArray

    val partitions = kSDA.divideSpace(dataset, 8)
    assert(partitions.length == 8)
  }

  test("Each partition bounding box contains only valid coordinates") {
    val dataset = (1 to 500).map { _ =>
      DataPoint(Array(Random.between(-100f, 100f), Random.between(-50f, 50f)), 0)
    }.toArray

    val partitions = kSDA.divideSpace(dataset, 5)

    for ((minCoords, maxCoords) <- partitions) {
      assert(minCoords.length == 2)
      assert(maxCoords.length == 2)
      for (i <- 0 until 2) {
        assert(minCoords(i) <= maxCoords(i), s"min > max in dimension $i")
      }
    }
  }

  test("Partitioning with 1 partition returns full bounding box") {
    val dataset = Array(
      DataPoint(Array(0f, 0f), 0),
      DataPoint(Array(1f, 1f), 1),
      DataPoint(Array(2f, 3f), 2)
    )

    val partitions = kSDA.divideSpace(dataset, 1)
    assert(partitions.length == 1)
    val (minCoords, maxCoords) = partitions(0)
    assert(minCoords.sameElements(Array(0f, 0f)))
    assert(maxCoords.sameElements(Array(2f, 3f)))
  }

  test("Partitioning single data point returns one partition with min == max") {
    val dp = DataPoint(Array(1.5f, -2.3f), 0)
    val partitions = kSDA.divideSpace(Array(dp), 1)
    val (minCoords, maxCoords) = partitions(0)
    assert(minCoords.sameElements(dp.coordinates))
    assert(maxCoords.sameElements(dp.coordinates))
  }

  test("Bounding boxes are consistent with input data range") {
    val dataset = (1 to 500).map { _ =>
      DataPoint(Array(Random.between(-10f, 10f), Random.between(100f, 200f)), 0)
    }.toArray

    val globalMin = dataset.map(_.coordinates).transpose.map(_.min)
    val globalMax = dataset.map(_.coordinates).transpose.map(_.max)

    val partitions = kSDA.divideSpace(dataset, 10)

    for ((minCoords, maxCoords) <- partitions) {
      for (i <- minCoords.indices) {
        assert(minCoords(i) >= globalMin(i) - 1e-5)
        assert(maxCoords(i) <= globalMax(i) + 1e-5)
      }
    }
  }


  test("Using a fixed seed produces reproducible splits") {
    val seed = 42
    val rand = new Random(seed)

    val dataset = (1 to 200).map { _ =>
      DataPoint(Array(rand.nextFloat(), rand.nextFloat()), 0)
    }.toArray

    val partitions1 = kSDA.divideSpace(dataset, 4, 42)
    val partitions2 = kSDA.divideSpace(dataset, 4, 42)

    assert(partitions1.length == partitions2.length)
    assert(partitions1.map { case (a, b) => (a.toSeq, b.toSeq) }.toSeq ==
      partitions2.map { case (a, b) => (a.toSeq, b.toSeq) }.toSeq)
  }

}
