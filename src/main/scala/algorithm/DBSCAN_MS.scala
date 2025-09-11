package algorithm

import model.DataPoint
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import utils.Distance.euclidean

object DBSCAN_MS {
  def run(filepath: String, numberOfPartitions: Int, seed: Int = 42, epsilon: Float, numberOfPivots: Int): Unit = {
    val pivots = HFI(null, numberOfPivots, euclidean, seed)
    val subspaces = kSDA(null, pivots, numberOfPartitions, seed, epsilon)

    val spark = SparkSession.builder().appName("Example").master("local[*]").getOrCreate()

    try {
      val data = readData(filepath)
      val rdd: RDD[(Int, DataPoint)] = spark.sparkContext.parallelize(data.flatMap(point => {
        kPA(point, pivots, subspaces)
      }))
      require(numberOfPartitions == subspaces.length, "Something has gone very wrong. Number of partitions does not match number of subspaces.")
      // TODO: Partitioning with HashPartitioner like this should work but check it something is wrong
      val partitionedRDD = rdd.partitionBy(new HashPartitioner(numberOfPartitions))
    }
  }

  private def readData(path: String): Array[DataPoint] = ???
}
