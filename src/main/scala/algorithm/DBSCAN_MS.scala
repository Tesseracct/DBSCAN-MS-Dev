package algorithm

import model.DataPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkContext}
import utils.Distance.euclidean

case object DBSCAN_MS {
  def run(filepath: String,
          numberOfPartitions: Int,
          seed: Int = 42,
          epsilon: Float,
          numberOfPivots: Int,
          samplingDensity: Double = 0.001): Unit = {
    val spark = SparkSession.builder().appName("Example").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    try {
      val rdd = readData(sc, filepath)

      val sampledRDD = rdd.sample(withReplacement = false, fraction = samplingDensity, seed = seed)
      val sampledData = sampledRDD.collect()

      val pivots = HFI(sampledData, numberOfPivots, euclidean, seed)
      val subspaces = kSDA(sampledData, pivots, numberOfPartitions, seed, epsilon)

      val bcPivots = sc.broadcast(pivots)
      val bcSubspaces = sc.broadcast(subspaces)

      val data: RDD[(Int, DataPoint)] = rdd.flatMap(kPA(_, bcPivots.value, bcSubspaces.value))

      require(numberOfPartitions == subspaces.length, "Something has gone very wrong. Number of partitions does not match number of subspaces.")
      // TODO: Partitioning with HashPartitioner like this should work but check it something is wrong
      val partitionedRDD = data.partitionBy(new HashPartitioner(numberOfPartitions))

      val clusteredRDD = partitionedRDD.mapPartitions(iter => {
        val partition = iter.map(_._2).toArray

        val pointsAndNeighbourhoods = SWNQA(partition, epsilon, seed)
      })




      partitionedRDD.collect().foreach(println)
    }
  }

  private def readData(sc: SparkContext, path: String): RDD[DataPoint] = {
    sc.textFile(path).zipWithIndex().map {case (line, index) => makeDataPoint(line, index)}
  }

  private def makeDataPoint(line: String, index: Long): DataPoint = {
    val data = line.split(",").map(_.toFloat)
    DataPoint(data, id = index)
  }
}
