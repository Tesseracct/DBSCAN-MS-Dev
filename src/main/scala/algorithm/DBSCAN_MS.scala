package algorithm

import model.{DataPoint, LABEL, MASK}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkContext}
import utils.Distance.euclidean

object DBSCAN_MS {
  /**
   * Executes the DBSCAN-MS clustering algorithm (Yang et al., 2019) on the given dataset using Spark.
   * The algorithm proceeds in three stages: (1) partitioning the data into balanced subspaces via sampling,
   * pivot selection, and k-d tree space division; (2) running local DBSCAN in each partition using a sliding
   * window neighborhood query; and (3) merging local clusters into global clusters via a connected components graph.
   *
   * Input must be a CSV file of numeric vectors, optionally with a header row and/or ground-truth labels in the last column.
   * The result is an array of `DataPoint` objects with assigned cluster IDs (globalCluster), where -1 denotes noise.
   *
   * @param filepath           The path to the input file containing the data to be clustered.
   * @param epsilon            The maximum distance two points can be apart to be considered neighbours.
   * @param minPts             The minimum number of points required to form a dense region.
   * @param numberOfPivots     The number of pivots to be used for subspace decomposition and neighbourhood search optimization.
   * @param numberOfPartitions The number of partitions for data distribution. Must be <= 4096.
   * @param samplingDensity    The fraction of data used for sampling operations. E.g., 0.01 is 1% of the data (default: 0.001).
   * @param seed               The random seed used for reproducibility of results (default: 42).
   * @param dataHasHeader      Indicates whether the input file contains a header row (default: false).
   * @param dataHasRightLabel  Indicates whether the input file contains ground truth labels for validation (default: false).
   * @return An array of `DataPoint` objects representing the clustered data.
   */
  def run(filepath: String,
          epsilon: Float,
          minPts: Int,
          numberOfPivots: Int,
          numberOfPartitions: Int,
          samplingDensity: Double = 0.001,
          seed: Int = 42,
          dataHasHeader: Boolean = false,
          dataHasRightLabel: Boolean = false): Array[DataPoint] = {
    require(numberOfPartitions < 4096, "Number of partitions must be < 2^12 (4096) because of how clusters are labeled.")

    val spark = SparkSession.builder()
      .appName("Example")
      .config("spark.local.dir", "S:\\temp")
      .master("local[14]") // * for all cores
      .config("spark.driver.memory", "4g").config("spark.driver.maxResultSize", "15g")
      .config("spark.executor.memory", "8g").getOrCreate()
    val sc = spark.sparkContext
    try {
      val rdd = readData(sc, filepath, dataHasHeader, dataHasRightLabel)
      val sampledData = rdd.sample(withReplacement = false, fraction = samplingDensity, seed = seed).collect()
      val clusteredRDD = dbscan_ms(sc, rdd, epsilon, minPts, seed, numberOfPivots, numberOfPartitions, sampledData)
      clusteredRDD.collect()
    }
    finally {
      spark.stop()
    }
  }

  private def dbscan_ms(sc: SparkContext,
                        rdd: RDD[DataPoint],
                        epsilon: Float,
                        minPts: Int,
                        seed: Int,
                        numberOfPivots: Int,
                        numberOfPartitions: Int,
                        sampledData: Array[DataPoint]): RDD[DataPoint] = {
    val pivots = HFI(sampledData, numberOfPivots, euclidean, seed)
    val subspaces = kSDA(sampledData, pivots, numberOfPartitions, seed, epsilon)

    val bcPivots = sc.broadcast(pivots)
    val bcSubspaces = sc.broadcast(subspaces)

    val data: RDD[(Int, DataPoint)] = rdd.flatMap(kPA(_, bcPivots.value, bcSubspaces.value))

    require(numberOfPartitions == subspaces.length, "Something has gone very wrong. Number of partitions does not match number of subspaces.")
    val partitionedRDD = data.partitionBy(new HashPartitioner(numberOfPartitions)).map(_._2)

    val clusteredRDD: RDD[DataPoint] = partitionedRDD.mapPartitions(iter => {
      val partition = iter.toArray
      val rng = new scala.util.Random(seed)
      val dimension = rng.nextInt(partition.head.dimensions)

      val sortedPartition = partition.sortBy(point => point.vectorRep(dimension))
      val neighbourhoods = SWNQA(sortedPartition, dimension, epsilon)

      localDBSCAN(sortedPartition, neighbourhoods, minPts).iterator
    })


    // As per DBSCAN-MS, Section VII: "we only need to transfer the core and border objects in the margins"
    // TODO: Check if Mask is necessary: no conditional for SPACE_INNER? => Margin conditional could be replaced by bool
    val mergingCandidates = clusteredRDD.filter(point =>
      (point.mask == MASK.MARGIN_OUTER || point.mask == MASK.MARGIN_INNER) &&
        (point.label == LABEL.CORE || point.label == LABEL.BORDER)).collect()
    val bcGlobalClusterMappings = sc.broadcast(CCGMA(mergingCandidates))

    // Merge local clusters into global clusters. If one cluster sits entirely in one partition,
    // we give it a unique ID by encoding the partition number in the high bits.
    val bitOffset = 19
    val mergedRDD = clusteredRDD.map(point => {
      bcGlobalClusterMappings.value.get((point.partition, point.localCluster)) match {
        case Some(cluster) => point.globalCluster = cluster
        case None => point.globalCluster = if (point.localCluster == -1) -1 else {
          ((point.partition + 1) << bitOffset) | point.localCluster
        }
      }
      point
    })

    // Eliminate false noise, that is, points labeled as noise in one partition but are part of a cluster in another partition.
    val clustered = mergedRDD.filter(_.globalCluster != -1).keyBy(_.id)
    val noise = mergedRDD.filter(_.globalCluster == -1).keyBy(_.id)
    val trueNoise = noise.subtractByKey(clustered).values
    clustered.values.union(trueNoise)
  }

  private def readData(sc: SparkContext, path: String, hasHeader: Boolean, hasRightLabel: Boolean): RDD[DataPoint] = {
    val rdd = sc.textFile(path).zipWithIndex()
    val rdd1 = if (hasHeader) rdd.filter(_._2 > 0) else rdd
    rdd1.map {case (line, index) => makeDataPoint(line, index, hasRightLabel)}
  }

  private def makeDataPoint(line: String, index: Long, hasRightLabel: Boolean): DataPoint = {
    val data = line.split(",").map(_.toFloat)
    val cleanedData = if (hasRightLabel) data.dropRight(1) else data
    DataPoint(cleanedData, id = index)
  }
}
