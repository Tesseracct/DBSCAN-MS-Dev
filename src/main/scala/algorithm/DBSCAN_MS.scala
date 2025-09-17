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
          seed: Int = 42,
          dataHasHeader: Boolean = false,
          dataHasRightLabel: Boolean = false): Array[DataPoint] = {
    val spark = SparkSession.builder().appName("Example").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    try {
      val rdd = readData(sc, filepath, dataHasHeader, dataHasRightLabel)

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

      val globalClusterMappings = CCGMA(mergingCandidates)
      val bcGlobalClusterMappings = sc.broadcast(globalClusterMappings)

      val mergedRDD = clusteredRDD.mapPartitions(iter => {
        val partition = iter.toArray
        val globalClusterMappings = bcGlobalClusterMappings.value

        partition.map(point => {
          globalClusterMappings.get((point.partition, point.localCluster)) match {
            case Some(cluster) => point.globalCluster = cluster
            case None => point.globalCluster = if (point.localCluster == -1) -1 else ((point.partition + 1) << 32) | point.localCluster
          }
          point
        }).iterator
      })

      mergedRDD.collect()
    }
    finally {
      spark.stop()
    }
  }

  private def readData(sc: SparkContext, path: String, hasHeader: Boolean, hasRightLabel: Boolean): RDD[DataPoint] = {
    val rdd = sc.textFile(path).zipWithIndex()

    val rdd1 = if (hasHeader) rdd.filter(_._2 > 0) else rdd

    val numLines = rdd1.count()
    val rdd2 = if (hasRightLabel) rdd1.filter(_._2 < numLines - 1) else rdd1

    rdd2.map {case (line, index) => makeDataPoint(line, index, hasRightLabel)}
  }

  private def makeDataPoint(line: String, index: Long, hasRightLabel: Boolean): DataPoint = {
    val data = line.split(",").map(_.toFloat)
    val cleanedData = if (hasRightLabel) data.dropRight(1) else data
    println(s"Make DataPoint: $index") //TODO: Remove
    DataPoint(cleanedData, id = index)
  }
}
