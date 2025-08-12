package algorithm

import model.DataPoint
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
import algorithm.PivotSelection.{HF, findFarthestPoint}
import utils.Distance.euclidean

class PivotSelectionTest() extends AnyFunSuite {
  // Test findFarthestPoint
  test("Test findFarthestPoint") {
    val seed = 42
    val rng = new Random(seed)
    val dataset: Array[DataPoint] = Array.fill(50)(DataPoint(Array(rng.nextFloat(), rng.nextFloat(), rng.nextFloat()), id = 0))

    val startingPoint = dataset(rng.nextInt(dataset.length))
    val farthestPoint = findFarthestPoint(dataset, startingPoint, euclidean)

    dataset.foreach { point =>
      assert(euclidean(startingPoint.coordinates, farthestPoint.coordinates) >= euclidean(startingPoint.coordinates, point.coordinates),
        s"Farthest point ${farthestPoint.coordinates.mkString(",")} is not actually the farthest from ${startingPoint.coordinates.mkString(",")}")
    }
  }


  private def recalcErrors(pivots: Array[DataPoint]): Array[Float] = {
    val edge = euclidean(pivots(0).coordinates, pivots(1).coordinates)
    pivots.zipWithIndex.drop(2).map { case (a: DataPoint, i: Int) =>
      pivots.take(i).map(b => Math.abs(edge - euclidean(a.coordinates, b.coordinates))).sum
    }
  }

  test("Basic HF functionality test in 2D") {
    val seed = 42
    val rng = new Random(seed)
    val dataset: Array[DataPoint] = Array.fill(20)(DataPoint(Array.fill(2)(rng.nextFloat()), id = 0))

    val result: Array[DataPoint] = HF(dataset, 4, euclidean, seed)
//    println(result.map(_.coordinates.mkString("Datapoint(", ", ", ")")).mkString("Array(", ", ", ")"))

    /*
    println(edge)
    println(euclidean(coords(0), coords(2)))
    println(euclidean(coords(1), coords(2)))
    println(euclidean(coords(0), coords(3)))
    println(euclidean(coords(1), coords(3)))
    println(euclidean(coords(2), coords(3)))
    */

    val errors = recalcErrors(result)
    assert(errors sameElements errors.sorted)
  }

  test("HF test in 20D") {
    val seed = 42
    val rng = new Random(seed)
    val dataset: Array[DataPoint] = Array.fill(10000)(DataPoint(Array.fill(20)(rng.nextFloat()), id = 0))

    val result: Array[DataPoint] = HF(dataset, 20, euclidean, seed)
    val errors = recalcErrors(result)
    assert(errors sameElements errors.sorted)
  }

}
