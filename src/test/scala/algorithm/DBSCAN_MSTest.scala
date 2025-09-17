package algorithm

import imp.MutualInformation.normalizedMutualInfoScore
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source
import scala.util.Using

class DBSCAN_MSTest extends AnyFunSuite{
  test("Test") {
    DBSCAN_MS.run("data/synth_data_points10000x3D.csv", epsilon = 0.03f, minPts = 3, numberOfPivots = 9, numberOfPartitions = 10, samplingDensity = 0.2f)
  }

  test("2D Clustering Test with synthetic data from Sci-Kit Learn") {
    val result = DBSCAN_MS.run("data/dbscan_dataset_2400x2D.csv",
      epsilon = 2f,
      minPts = 3,
      numberOfPivots = 9,
      numberOfPartitions = 10,
      samplingDensity = 0.3f,
      dataHasHeader = true,
      dataHasRightLabel = true)

    val reducedResult = result.map(p => (p.id, p.globalCluster)).distinct
    val numberOfPoints = result.map(_.id).distinct
    val index = reducedResult.map(_._2).zipWithIndex.toMap
    val remappedResult = result.map(p => {
      val newLabel = index.get(p.globalCluster)
      newLabel match {
        case Some(newLabel) => newLabel
        case _ => throw new Exception("WTF")
      }
    }).sorted

    val checkingLabels = getRightmostColumn("data/dbscan_dataset_2400x2D.csv").toArray.map(_.toFloat.toInt).sorted

    println(numberOfPoints.length)
    println(reducedResult.length)
    println(checkingLabels.length)

    val numberOfPointsSorted = numberOfPoints.sorted
    println(numberOfPointsSorted.mkString("Array(", ", ", ")"))


//    println(normalizedMutualInfoScore(labelsTrue = checkingLabels, labelsPred = remappedResult))
  }




  def getRightmostColumn(filePath: String): Seq[String] = {
    Using(Source.fromFile(filePath)) { source =>
      source.getLines().drop(1).map(_.split(',').last).toSeq
    }.getOrElse(Seq.empty)
  }
}
