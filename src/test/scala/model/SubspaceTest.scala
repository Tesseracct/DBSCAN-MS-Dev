package model

import algorithm.kSDA
import org.scalatest.funsuite.AnyFunSuite

class SubspaceTest extends AnyFunSuite {

  test("Subspace split should correctly divide points and update bounding box") {
    val points = Array(
      DataPoint(Array(1.0f, 2.0f), id = 1),
      DataPoint(Array(3.0f, 4.0f), id = 2),
      DataPoint(Array(5.0f, 6.0f), id = 3),
      DataPoint(Array(7.0f, 8.0f), id = 4)
    )

  }

}
