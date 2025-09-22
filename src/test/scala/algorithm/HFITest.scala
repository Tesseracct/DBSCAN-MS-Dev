package algorithm

import model.DataPoint
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
import utils.Distance.euclidean

class HFITest() extends AnyFunSuite {
  // Test HFI
  test("Basic HFI tests") {
    val seed = 42
    val rng = new Random(seed)
    val dataset: Array[DataPoint] = Array.fill(1000)(DataPoint(Array.fill(5)(rng.nextFloat()), id = 0))

    val numberOfPivots = 10
    val pivots: Array[DataPoint] = HFI(dataset, numberOfPivots, euclidean, seed)

    // Check that the correct number of pivots is returned
    assert(pivots.length == numberOfPivots, s"Expected $numberOfPivots pivots, but got ${pivots.length}")

    // Check that all pivots are from the original dataset
    pivots.foreach { pivot =>
      assert(dataset.contains(pivot), s"Pivot ${pivot.data.mkString(",")} is not in the original dataset")
    }

    // Check that all pivots are unique
    assert(pivots.distinct.length == pivots.length, "Pivots are not unique")


  }

  test("Semantic HFI test") {
    val seed = 42
    val rng = new Random(seed)
    val dataset: Array[DataPoint] = Array.fill(10)(DataPoint(Array.fill(2)(rng.nextFloat()), id = 0))

    val numberOfPivots = 3
    val pivots: Array[DataPoint] = HFI(dataset, numberOfPivots, euclidean, seed)

    // Print the selected pivots for manual inspection
    println("Selected pivots:")
    pivots.foreach(p => println(p.data.mkString("(", ", ", ")")))
  }


  // Test newPivotSetPrecision
  test("Test newPivotSetPrecision") {
  }


  // Test L_infNorm
  test("Test L_infNorm") {
    val vec1 = List(1.0f, 2.0f, 3.0f)
    val vec2 = List(4.0f, 0.0f, -1.0f)
    val expected1 = 4.0f // max(|1-4|, |2-0|, |3-(-1)|) = max(3, 2, 4) = 4
    val result1 = HFI.L_infNorm(vec1, vec2)
    assert(result1 == expected1, s"Expected $expected1 but got $result1")

    val vec3 = List(0.0f, 0.0f, 0.0f, 0.0f)
    val result2 = HFI.L_infNorm(vec3, vec3)
    assert(result2 == 0.0f, s"Expected 0.0 but got $result2")

    val vec4 = List(-2.0f, -2.0f, -9.0f)
    val vec5 = List(-5.0f, -3.0f, -1.5f)
    val expected3 = 7.5f // max(|-2+5|, |-2+3|, |-9+1.5|) = max(3, 1, 7.5) = 7.5
    val result3 = HFI.L_infNorm(vec4, vec5)
    assert(result3 == expected3, s"Expected $expected3 but got $result3")
  }


  // Test samplePairs
  test("Test samplePairs") {
    val seed = 42
    val rng = new Random(seed)
    val dataset: Array[DataPoint] = Array.fill(100)(DataPoint(Array.fill(3)(rng.nextFloat()), id = 0))

    val numberOfPairs = 20
    val pairs: Array[(DataPoint, DataPoint)] = HFI.samplePairs(dataset, numberOfPairs, seed)

    assert(pairs.length == numberOfPairs, s"Expected $numberOfPairs pairs, but got ${pairs.length}")

    pairs.foreach { case (dp1, dp2) =>
      assert(dp1 != dp2, "A pair contains the same DataPoint twice")
    }
  }


}
