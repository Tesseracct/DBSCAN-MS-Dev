package algorithm

import algorithm.HF.findFarthestPoint
import model.DataPoint
import org.scalatest.funsuite.AnyFunSuite
import utils.EuclideanDistance.distance

import scala.util.Random

class HFTest extends AnyFunSuite{
  // Test findFarthestPoint
  test("Test findFarthestPoint") {
    val seed = 42
    val rng = new Random(seed)
    val dataset: Array[DataPoint] = Array.fill(50)(DataPoint(Array(rng.nextFloat(), rng.nextFloat(), rng.nextFloat()), id = 0))

    val startingPoint = dataset(rng.nextInt(dataset.length))
    val farthestPoint = findFarthestPoint(dataset, startingPoint)

    dataset.foreach { point =>
      assert(distance(startingPoint.data, farthestPoint.data) >= distance(startingPoint.data, point.data),
        s"Farthest point ${farthestPoint.data.mkString(",")} is not actually the farthest from ${startingPoint.data.mkString(",")}")
    }
  }


  // Test HF
  private def recalcErrors(pivots: Array[DataPoint]): Array[Float] = {
    val edge = distance(pivots(0).data, pivots(1).data)
    pivots.zipWithIndex.drop(2).map { case (a: DataPoint, i: Int) =>
      pivots.take(i).map(b => Math.abs(edge - distance(a.data, b.data))).sum
    }
  }

  test("Basic HF functionality test in 2D") {
    val seed = 42
    val rng = new Random(seed)
    val dataset: Array[DataPoint] = Array.fill(20)(DataPoint(Array.fill(2)(rng.nextFloat()), id = 0))

    val result: Array[DataPoint] = HF(dataset, 4, seed)

    val errors = recalcErrors(result)
    assert(errors sameElements errors.sorted)
  }

  test("HF test in 20D") {
    val seed = 42
    val rng = new Random(seed)
    val dataset: Array[DataPoint] = Array.fill(10000)(DataPoint(Array.fill(20)(rng.nextFloat()), id = 0))

    val result: Array[DataPoint] = HF(dataset, 20, seed)
    val errors = recalcErrors(result)
    assert(errors sameElements errors.sorted)
  }

  test("Detailed HF test") {
    val seed = 42
    val firstIntFromRNG = 5
    val dataset: Array[DataPoint] = Array(
      DataPoint(Array(0.7275637f, 0.054665208f), id = 0),
      DataPoint(Array(0.6832234f, 0.0479393f), id = 1),
      DataPoint(Array(0.3087194f, 0.9420735f), id = 2),
      DataPoint(Array(0.27707845f, 0.70771056f), id = 3),
      DataPoint(Array(0.6655489f, 0.09132457f), id = 4),
      DataPoint(Array(0.9033722f, 0.45125717f), id = 5),
      DataPoint(Array(0.36878288f, 0.38164306f), id = 6),
      DataPoint(Array(0.275748f, 0.69042575f), id = 7),
      DataPoint(Array(0.46365356f, 0.76209015f), id = 8),
      DataPoint(Array(0.78290176f, 0.98817854f), id = 9),
      DataPoint(Array(0.91932774f, 0.15195823f), id = 10),
      DataPoint(Array(0.43649095f, 0.4397998f), id = 11),
      DataPoint(Array(0.7499061f, 0.93063116f), id = 12),
      DataPoint(Array(0.38656682f, 0.7982866f), id = 13),
      DataPoint(Array(0.17737848f, 0.15054744f), id = 14))

    val numberOfPivots = 5
    val pivotCandidates: Array[DataPoint] = HF(dataset, numberOfPivots, seed)


    // Check that rng is in order and first pivot candidate is correct
    assert(pivotCandidates(0) == findFarthestPoint(dataset, dataset(firstIntFromRNG)))

    // Now check all pivot candidates for correctness
    val expectedCandidates: Array[Int] = Array(14, 9, 10, 2, 0)
    expectedCandidates.zipWithIndex.foreach { case (expectedId, i) =>
      assert(pivotCandidates(i) == dataset(expectedId), s"Pivot candidate at index $i is incorrect")
    }
  }
}
