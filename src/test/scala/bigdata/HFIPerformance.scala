package bigdata

import algorithm.HFI
import model.DataPoint
import org.scalatest.funsuite.AnyFunSuite
import utils.EuclideanDistance.distance

import scala.util.Random

class HFIPerformance extends AnyFunSuite{
  test("Performance test") {
    val seed = 42
    val rng = new Random(seed)

    // Read data from csv file
    val source = scala.io.Source.fromFile("data/combined_circles_moons_noise.csv")
    val rawData = source.getLines().drop(1).map(line => line.split(",").map(_.toFloat)).toArray
    source.close()
    val cleanedData = rawData.map(row => row.dropRight(1))
    val dataset = cleanedData.zipWithIndex.map { case (row, id) => DataPoint(row, id) }

    val start = System.currentTimeMillis()
    val result = HFI(dataset, 10, seed)
    val end = System.currentTimeMillis()
    println(s"Time in seconds: ${(end - start) / 1000.0}")

  }
}
