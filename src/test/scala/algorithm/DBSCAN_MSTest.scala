package algorithm

import imp.MutualInformation.normalizedMutualInfoScore
import org.scalatest.funsuite.AnyFunSuite
import utils.{GetResultLabels, Testing}


class DBSCAN_MSTest extends AnyFunSuite{
//  test("Test") {
//    val filepath = "data/synth_data_points10000x3D.csv"
//    val result = DBSCAN_MS.run(filepath,
//      epsilon = 0.05f,
//      minPts = 3,
//      numberOfPivots = 9,
//      numberOfPartitions = 10,
//      samplingDensity = 0.1f)
//
//    val points = result.map(p => (p.id, p.globalCluster)).distinct
//    val clusters = points.groupBy(_._2)
//
//    println(clusters.size)
//  }

  test("100 x 2D Synthetic") {
    val filepath = "data/dbscan_dataset_100x2D.csv"
    val result = DBSCAN_MS.run(filepath,
      epsilon = 1.5f,
      minPts = 5,
      numberOfPivots = 9,
      numberOfPartitions = 10,
      samplingDensity = 0.5f,
      dataHasHeader = true,
      dataHasRightLabel = true)

    val (originalData, labelsTrue) = Testing.splitData(Testing.readDataToString(filepath, header = true))
    val predLabels = GetResultLabels(result, originalDataset = Option(originalData))

    // Note, this dataset is useless and to be used cautiously.
    assert(normalizedMutualInfoScore(labelsTrue,predLabels) > .9d)
  }

  test("Moons 2500 x 2D no Noise") {
    val filepath = "data/moons_2500x2D.csv"
    val result = DBSCAN_MS.run(filepath,
      epsilon = 0.1f,
      minPts = 5,
      numberOfPivots = 10,
      numberOfPartitions = 10,
      samplingDensity = 0.2f,
      dataHasHeader = true,
      dataHasRightLabel = true)

    val (originalData, labelsTrue) = Testing.splitData(Testing.readDataToString(filepath, header = true))
    val predLabels = GetResultLabels(result, originalDataset = Option(originalData))

    assert(normalizedMutualInfoScore(labelsTrue,predLabels) == 1.0d)
    assert(predLabels.distinct.length == 2)
  }

  test("Circles 2500 x 2D no Noise") {
    val filepath = "data/circles_2500x2D.csv"
    val result = DBSCAN_MS.run(filepath,
      epsilon = 0.1f,
      minPts = 5,
      numberOfPivots = 10,
      numberOfPartitions = 10,
      samplingDensity = 0.2f,
      dataHasHeader = true,
      dataHasRightLabel = true)

    val (originalData, labelsTrue) = Testing.splitData(Testing.readDataToString(filepath, header = true))
    val predLabels = GetResultLabels(result, originalDataset = Option(originalData))

    assert(normalizedMutualInfoScore(labelsTrue,predLabels) == 1.0d)
    assert(predLabels.distinct.length == 2)
  }

  test("Blobs 1000 x 2D no Noise") {
    val filepath = "data/blobs_1000x2D.csv"
    val result = DBSCAN_MS.run(filepath,
      epsilon = 1.3f,
      minPts = 5,
      numberOfPivots = 10,
      numberOfPartitions = 10,
      samplingDensity = 0.2f,
      dataHasHeader = true,
      dataHasRightLabel = true)

    val (originalData, labelsTrue) = Testing.splitData(Testing.readDataToString(filepath, header = true))
    val predLabels = GetResultLabels(result, originalDataset = Option(originalData))

    assert(normalizedMutualInfoScore(labelsTrue,predLabels) == 1.0d)
    assert(predLabels.distinct.length == 4)
  }

  test("Combined Circles & Moons 5000 x 2D no Noise") {
    val filepath = "data/combined_circles_moons.csv"
    val result = DBSCAN_MS.run(filepath,
      epsilon = 0.1f,
      minPts = 5,
      numberOfPivots = 10,
      numberOfPartitions = 10,
      samplingDensity = 0.15f,
      dataHasHeader = true,
      dataHasRightLabel = true)

    val (originalData, labelsTrue) = Testing.splitData(Testing.readDataToString(filepath, header = true))
    val predLabels = GetResultLabels(result, originalDataset = Option(originalData))

    assert(normalizedMutualInfoScore(labelsTrue,predLabels) == 1.0d)
    assert(predLabels.distinct.length == 4)
  }

  test("Combined Circles & Moons 5010 x 2D with 10 Noise Points") {
    val filepath = "data/combined_circles_moons_noise.csv"
    val result = DBSCAN_MS.run(filepath,
      epsilon = 0.1f,
      minPts = 5,
      numberOfPivots = 10,
      numberOfPartitions = 10,
      samplingDensity = 0.15f,
      dataHasHeader = true,
      dataHasRightLabel = true)

    val (originalData, labelsTrue) = Testing.splitData(Testing.readDataToString(filepath, header = true))
    val predLabels = GetResultLabels(result, originalDataset = Option(originalData))

    assert(predLabels.count(_ == -1) == 10)
    assert(normalizedMutualInfoScore(labelsTrue,predLabels) == 1.0d)
    assert(predLabels.distinct.length == 5)
  }



}
