package algorithm

import imp.MutualInformation.normalizedMutualInfoScore
import org.scalatest.funsuite.AnyFunSuite
import utils.{GetResultLabels, Testing, WriteResultToCSV}

import scala.io.Source
import scala.util.Using

class DBSCAN_MSTest extends AnyFunSuite{
  test("Test") {
    DBSCAN_MS.run("data/synth_data_points10000x3D.csv",
      epsilon = 0.03f,
      minPts = 3,
      numberOfPivots = 9,
      numberOfPartitions = 10,
      samplingDensity = 0.2f)
  }

  test("2D Clustering Test with synthetic data from Sci-Kit Learn") {
    val result = DBSCAN_MS.run("data/dbscan_dataset_100x2D.csv",
      epsilon = 1.5f,
      minPts = 5,
      numberOfPivots = 9,
      numberOfPartitions = 10,
      samplingDensity = 0.5f,
      dataHasHeader = true,
      dataHasRightLabel = true)

    val distinctResult = result.map(p => (p.id, p.globalCluster)).distinct
    val distinctClusters = distinctResult.filterNot(_._2 == -1)
    val newLabelsMapping = distinctClusters.map(_._2).distinct.zipWithIndex.toMap
    val remappedClusters = distinctClusters.map(p => {
      val newLabel = newLabelsMapping.get(p._2)
      newLabel match {
        case Some(newLabel) => newLabel
        case _ => throw new Exception("WTF")
      }
    })
    val finalResult = remappedClusters.concat(distinctResult.filter(_._2 == -1).map(_ => -1)).sorted

    println(s"Cluster Mapping: ${distinctClusters.map(_._2).distinct.zipWithIndex.mkString("Array(", ", ", ")")}")
    println(s"Distinct Results: ${distinctResult.length}")
    println(s"Amount of Points in Clusters: ${remappedClusters.length}")
    println(s"Final Result length: ${finalResult.length}")

    //    distinctResult.zipWithIndex.foreach(t => println(s"${t._2}: ID: ${t._1._1}, GCluster: ${t._1._2}"))
    val duplicates = distinctResult.map(_._1).groupBy(identity).collect({
      case (id, amount) if amount.length > 1 => id
    })
    duplicates.foreach(println)
    println(s"Duplicate Amount: ${duplicates.size}")

    //    WriteResultToCSV(result, "data/dbscan_dataset_100x2D_result.csv")

    val checkingLabels = Testing.getRightmostColumn("data/dbscan_dataset_100x2D.csv").toArray.map(_.toFloat.toInt).sorted
    println(s"Normalized Mutual Information Score: ${normalizedMutualInfoScore(labelsTrue = checkingLabels, labelsPred = finalResult)}")

  }

  test("Moons 2500 x 2D no Noise") {
    val result = DBSCAN_MS.run("data/moons_2500x2D.csv",
      epsilon = 0.1f,
      minPts = 5,
      numberOfPivots = 10,
      numberOfPartitions = 10,
      samplingDensity = 0.2f,
      dataHasHeader = true,
      dataHasRightLabel = true)

    val distinctResult = result.map(p => (p.id, p.globalCluster)).distinct
    val distinctClusters = distinctResult.filterNot(_._2 == -1)
    val newLabelsMapping = distinctClusters.map(_._2).distinct.zipWithIndex.toMap
    val remappedClusters = distinctClusters.map(p => {
      val newLabel = newLabelsMapping.get(p._2)
      newLabel match {
        case Some(newLabel) => newLabel
        case _ => throw new Exception("WTF")
      }
    })

    val finalResult = remappedClusters.concat(distinctResult.filter(_._2 == -1).map(_ => -1)).sorted
    val checkingLabels = Testing.getRightmostColumn("data/moons_2500x2D.csv").toArray.map(_.toFloat.toInt).sorted
    val mInfoScore = normalizedMutualInfoScore(labelsTrue = checkingLabels, labelsPred = finalResult)

    println(s"Cluster Mapping: ${distinctClusters.map(_._2).distinct.zipWithIndex.mkString("Array(", ", ", ")")}")
    println(s"Normalized Mutual Information Score: $mInfoScore")

    assert(mInfoScore == 1.0f)
    assert(distinctClusters.map(_._2).distinct.length == 2)

  }

  test("Second Moon Test") {
    val result = DBSCAN_MS.run("data/moons_2500x2D_2.csv",
      epsilon = 0.1f,
      minPts = 5,
      numberOfPivots = 10,
      numberOfPartitions = 10,
      samplingDensity = 0.2f,
      dataHasHeader = true,
      dataHasRightLabel = true)

    val (features, labelsTrue) = Testing.splitData(Testing.readDataToString("data/moons_2500x2D_2.csv"))

    val predLabels = GetResultLabels(result, remappedLabels = true, originalDataset = labelsTrue)


  }


}
