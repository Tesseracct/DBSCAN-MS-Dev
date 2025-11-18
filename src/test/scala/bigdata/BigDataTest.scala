package bigdata

import algorithm.DBSCAN_MS
import org.scalatest.funsuite.AnyFunSuite
import testutils.{GetResultLabels, TestSparkSession}


class BigDataTest extends AnyFunSuite{
  // VM config: -Xmx32g
  test("densired_2") {
    val filepath = "data/densired_2.csv"
    val spark = TestSparkSession.getOrCreate()
    try {
      val result = DBSCAN_MS.runFromFile(spark,
        filepath,
        epsilon = 0.05f,
        minPts = 20,
        numberOfPivots = 10,
        numberOfPartitions = 10,
        samplingDensity = 0.001f,
        dataHasHeader = false,
        dataHasRightLabel = false)

      val predLabels = GetResultLabels(result)
      val groupedLabels = predLabels.groupBy(identity)
      groupedLabels.foreach(x => println(s"Cluster ${x._1} has ${x._2.length} points"))
      println(s"Total clusters (including noise): ${groupedLabels.size}")
    }
    finally {
      spark.stop()
    }
  }

  test("densired_2_sampled50p") {
    val filepath = "data/densired_2_sampled50p.csv"
    val spark = TestSparkSession.getOrCreate()
    try {
      val result = DBSCAN_MS.runFromFile(spark,
        filepath,
        epsilon = 0.07f,
        minPts = 20,
        numberOfPivots = 10,
        numberOfPartitions = 10,
        samplingDensity = 0.001f)

      val predLabels = GetResultLabels(result)
      val groupedLabels = predLabels.groupBy(identity)
      groupedLabels.foreach(x => println(s"Cluster ${x._1} has ${x._2.length} points"))
      println(s"Total clusters (including noise): ${groupedLabels.size}")
    }
    finally {
      spark.stop()
    }
  }

  test("Moons 2500 x 2D no Noise") {
    val filepath = "data/moons_2500x2D.csv"
    val spark = TestSparkSession.getOrCreate()
    try {
      val result = DBSCAN_MS.runFromFile(spark,
        filepath,
        epsilon = 0.1f,
        minPts = 5,
        numberOfPivots = 10,
        numberOfPartitions = 10,
        samplingDensity = 0.2f,
        dataHasHeader = true,
        dataHasRightLabel = true)

      val predLabels = GetResultLabels(result)
      val groupedLabels = predLabels.groupBy(identity)
      assert(groupedLabels.size == 2)
      groupedLabels.foreach(x => assert(x._2.length == 1250))
    }
    finally {
      spark.stop()
    }
  }

    test("DataPoint size test") {
      val dataPoints = (1 to 20000000).map(i => model.DataPoint(Array.fill(2)(i.toFloat), -1, vectorRep = Array.fill(2)(i.toFloat))).toArray

      //    assert(dataPoints.length == 2000000)
      dataPoints.foreach(println)
    }
}

