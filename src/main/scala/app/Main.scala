package app

import algorithm.DBSCAN_MS
import org.apache.spark.sql.SparkSession
import testutils.GetResultLabels.printClusters
import testutils.{GetResultLabels, TestSparkSession}

import scala.util.{Failure, Success, Try}

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      Console.err.println(
        s"""
           |Usage: Main <filepath> <epsilon> <minPts> <numberOfPivots> <numberOfPartitions> <samplingDensity> <seed> <dataHasHeader> <dataHasRightLabel>
           |
           |Example:
           |  spark-submit --class app.Main target/dbscanms-assembly.jar data/input.csv 0.5 5 10 8 0.01 42 true false
         """.stripMargin)
      System.exit(1)
    }

    // Parse arguments
    val maybeArgs = for {
      filepath          <- Try(args(0))
      epsilon           <- Try(args(1).toFloat)
      minPts            <- Try(args(2).toInt)
      numberOfPivots    <- Try(args(3).toInt)
      numberOfPartitions<- Try(args(4).toInt)
      samplingDensity   <- Try(args(5).toFloat)
      seed              <- Try(args(6).toInt)
      dataHasHeader     <- Try(args(7).toBoolean)
      dataHasRightLabel <- Try(args(8).toBoolean)
    } yield (filepath, epsilon, minPts, numberOfPivots, numberOfPartitions, samplingDensity, seed, dataHasHeader, dataHasRightLabel)

    maybeArgs match {
      case Success((filepath, epsilon, minPts, numberOfPivots, numberOfPartitions, samplingDensity, seed, dataHasHeader, dataHasRightLabel)) =>
        Console.out.println(
          s"""
             |--- DBSCAN-MS Configuration ---
             |Filepath:             $filepath
             |Epsilon:              $epsilon
             |MinPts:               $minPts
             |Number of Pivots:     $numberOfPivots
             |Number of Partitions: $numberOfPartitions
             |Sampling Density:     $samplingDensity
             |Seed:                 $seed
             |Data Has Header:      $dataHasHeader
             |Data Has RightLabel:  $dataHasRightLabel
           """.stripMargin)

        val spark = SparkSession.builder().appName("DBSCAN-MS").getOrCreate()

        try {
          val start = System.nanoTime()

          val data = DBSCAN_MS.runFromFile(spark,
                                          filepath,
                                          epsilon,
                                          minPts,
                                          numberOfPivots,
                                          numberOfPartitions,
                                          samplingDensity,
                                          seed,
                                          dataHasHeader,
                                          dataHasRightLabel)

          val end = System.nanoTime()
          val duration = (System.nanoTime() - start) / 1e9
          printClusters(data)
          Console.out.println(f"Completed in $duration%.2f seconds")
        }
        finally {
          spark.stop()
        }

      case Failure(ex) =>
        Console.err.println(s"Error parsing arguments: ${ex.getMessage}")
        System.exit(1)
    }
  }
}
