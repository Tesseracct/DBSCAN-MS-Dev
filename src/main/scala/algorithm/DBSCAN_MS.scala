package algorithm

import model.{DataPoint, LABEL, MASK}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkContext}
import utils.Distance.euclidean

case object DBSCAN_MS {
  def run(filepath: String,
          epsilon: Float,
          minPts: Int,
          numberOfPivots: Int,
          numberOfPartitions: Int,
          samplingDensity: Double = 0.001,
          seed: Int = 42): Unit = {
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
      val partitionedRDD = data.partitionBy(new HashPartitioner(numberOfPartitions)).map(_._2)

      val clusteredRDD: RDD[DataPoint] = partitionedRDD.mapPartitions(iter => {
        val partition = iter.toArray
        val rng = new scala.util.Random(seed)
        val dimension = rng.nextInt(partition.head.dimensions)

        val sortedPartition = partition.sortBy(point => point.vectorRep(dimension))
        val neighbourhoods = SWNQA(sortedPartition, dimension, epsilon)

        DBSCAN(sortedPartition, neighbourhoods, minPts).iterator
      })

      // As per DBSCAN-MS, Section VII: "we only need to transfer the core and border objects in the margins"
      // TODO: Check if Mask is necessary: no conditional for SPACE_INNER? => Margin conditional could be replaced by bool
      val mergingCandidates = clusteredRDD.filter(point =>
        (point.mask == MASK.MARGIN_OUTER || point.mask == MASK.MARGIN_INNER) &&
          (point.label == LABEL.CORE || point.label == LABEL.BORDER)).collect()

      CCGMA(mergingCandidates)


      clusteredRDD.collect().foreach(println)
    }
    finally {
      spark.stop()
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
