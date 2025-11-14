package algorithm

import metrics.MetricWriter
import metrics.entity.{ClusterParameters, DatasetParameters, Measurement}
import model.{DataPoint, LABEL, MASK}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkContext, TaskContext}
import utils.IntrinsicDimensionality

import java.nio.file.Path
import java.time.LocalTime

object DBSCAN_MS {
  private val log = org.apache.log4j.LogManager.getLogger(DBSCAN_MS.getClass)

  /**
   * Convenience wrapper for [[run]] that reads the dataset from a file.
   *
   * This method performs the complete clustering pipeline:
   * reading input data, executing the DBSCAN-MS algorithm,
   * and collecting the clustered points to the driver.
   *
   * Input must be a CSV file of numeric vectors, optionally with a header row and/or ground-truth labels in the last column.
   *
   * @param spark              The SparkSession to use for execution.
   * @param filepath           The path to the input file containing the data to be clustered.
   * @param epsilon            The maximum distance two points can be apart to be considered neighbours.
   * @param minPts             The minimum number of points required to form a dense region.
   * @param numberOfPivots     The number of pivots to be used for subspace decomposition and neighbourhood search optimization.
   * @param numberOfPartitions The number of partitions for data distribution. Must be < 4096.
   * @param samplingDensity    The fraction of data used for sampling operations. E.g., 0.01 is 1% of the data (default: 0.001).
   * @param seed               The random seed used for reproducibility of results. Default: `42`.
   * @param dataHasHeader      Indicates whether the input file contains a header row (default: false).
   * @param dataHasRightLabel  Indicates whether the input file contains ground truth labels for validation (default: false).
   * @return An array of [[DataPoint]] objects representing the clustered data.
   * @see [[run]] for the underlying algorithm.
   */
  def runFromFile(spark: SparkSession,
                  filepath: String,
                  epsilon: Float,
                  minPts: Int,
                  numberOfPivots: Int,
                  numberOfPartitions: Int,
                  samplingDensity: Double = 0.001,
                  seed: Int = 42,
                  dataHasHeader: Boolean = false,
                  dataHasRightLabel: Boolean = false): Array[DataPoint] = {
    val sc = spark.sparkContext
    val rdd = readData(sc, filepath, dataHasHeader, dataHasRightLabel).cache()
    val numPivots = if (numberOfPivots == -1) estimatePivotNumber(rdd, samplingDensity, seed) else numberOfPivots

    val start = System.nanoTime()

    val clusteredRDD = run(sc,
      rdd,
      epsilon,
      minPts,
      numPivots,
      numberOfPartitions,
      samplingDensity,
      seed)
    val result = clusteredRDD.collect()

    val end = System.nanoTime()
    val duration = (end - start) / 1e9
    Console.out.println(f"Completed in $duration%.2f seconds")

    result
  }

  /**
   * Convenience wrapper for [[run]] that only performs a count at the end without collecting the results.
   *
   * Input must be a CSV file of numeric vectors, optionally with a header row and/or ground-truth labels in the last column.
   *
   * @param spark              The SparkSession to use for execution.
   * @param filepath           The path to the input file containing the data to be clustered.
   * @param epsilon            The maximum distance two points can be apart to be considered neighbours.
   * @param minPts             The minimum number of points required to form a dense region.
   * @param numberOfPivots     The number of pivots to be used for subspace decomposition and neighbourhood search optimization.
   * @param numberOfPartitions The number of partitions for data distribution. Must be < 4096.
   * @param samplingDensity    The fraction of data used for sampling operations. E.g., 0.01 is 1% of the data (default: 0.001).
   * @param seed               The random seed used for reproducibility of results. Default: `42`.
   * @param metricsPath        The path to the directory where metrics will be written. Default: empty string, which means no metrics will be written.
   * @param dataHasHeader      Indicates whether the input file contains a header row (default: false).
   * @param dataHasRightLabel  Indicates whether the input file contains ground truth labels for validation (default: false).
   * @see [[run]] for the underlying algorithm.
   */
  def runWithoutCollect(spark: SparkSession,
                        filepath: String,
                        epsilon: Float,
                        minPts: Int,
                        numberOfPivots: Int,
                        numberOfPartitions: Int,
                        samplingDensity: Double = 0.001,
                        seed: Int = 42,
                        metricsPath: String = "",
                        dataHasRightLabel: Boolean = false,
                        dataHasHeader: Boolean = false): Unit = {
    val sc = spark.sparkContext
    val rdd = readData(sc, filepath, dataHasHeader, dataHasRightLabel).cache()
    val numPivots = if (numberOfPivots == -1) estimatePivotNumber(rdd, samplingDensity, seed) else numberOfPivots
    log.info(rdd.count() + " data points loaded")

    val start = System.currentTimeMillis()
    val count = run(sc,
      rdd,
      epsilon,
      minPts,
      numPivots,
      numberOfPartitions,
      samplingDensity,
      seed
    ).count()
    val end = System.currentTimeMillis()
    log.info(f"Count: $count")
    val duration = (end - start) / 1000D / 60D
    log.info(f"DBSCAN-MS completed in ${duration.toInt} minutes.")
    val writer = new MetricWriter(Path.of(metricsPath))
    val datasetName = filepath.substring(filepath.lastIndexOf('/') + 1, filepath.lastIndexOf('.'))
    val measurement = new Measurement[ClusterParameters, DatasetParameters]("DBSCAN-MS",
                                                                            end - start,
                                                                            new ClusterParameters(epsilon, minPts),
                                                                            new DatasetParameters(datasetName))
    writer.writeMetrics(measurement)

  }

  /**
   * Executes the DBSCAN-MS clustering algorithm (Yang et al., 2019) on the given dataset using Spark.
   * The algorithm proceeds in three stages: (1) partitioning the data into balanced subspaces via sampling,
   * pivot selection, and k-d tree space division; (2) running local DBSCAN in each partition using a sliding
   * window neighborhood query; and (3) merging local clusters into global clusters via a connected components graph.
   *
   * The result is an RDD of [[DataPoint]] objects with assigned cluster IDs (globalCluster), where -1 denotes noise.
   *
   * @param sc                 The SparkContext to use for execution.
   * @param rdd                The RDD containing the data to be clustered.
   * @param epsilon            The maximum distance two points can be apart to be considered neighbours.
   * @param minPts             The minimum number of points required to form a dense region.
   * @param numberOfPivots     The number of pivots to be used for subspace decomposition and neighbourhood search optimization.
   * @param numberOfPartitions The number of partitions for data distribution. Must be < 4096.
   * @param samplingDensity    The fraction of data used for sampling operations. E.g., 0.01 is 1% of the data (default: 0.001).
   * @param seed               The random seed used for reproducibility of results. Default: `42`.
   * @return An RDD of [[DataPoint]] objects representing the clustered data.
   */
  def run(sc: SparkContext,
          rdd: RDD[DataPoint],
          epsilon: Float,
          minPts: Int,
          numberOfPivots: Int,
          numberOfPartitions: Int,
          samplingDensity: Double = 0.001,
          seed: Int = 42): RDD[DataPoint] = {
    require(numberOfPartitions < 4096, "Number of partitions must be < 2^12 (4096) because of how clusters are labeled.")

    val sampledData = rdd.sample(withReplacement = false, fraction = samplingDensity, seed = seed).collect()
    val clusteredRDD = dbscan_ms(sc, rdd, epsilon, minPts, seed, numberOfPivots, numberOfPartitions, sampledData)
    clusteredRDD
  }

  private def dbscan_ms(sc: SparkContext,
                        rdd: RDD[DataPoint],
                        epsilon: Float,
                        minPts: Int,
                        seed: Int,
                        numberOfPivots: Int,
                        numberOfPartitions: Int,
                        sampledData: Array[DataPoint]): RDD[DataPoint] = {
    val pivots = HFI(sampledData, numberOfPivots, seed)
    val subspaces = kSDA(sampledData, pivots, numberOfPartitions, seed, epsilon)

    val bcPivots = sc.broadcast(pivots)
    val bcSubspaces = sc.broadcast(subspaces)

    val data: RDD[(Int, DataPoint)] = rdd.flatMap(kPA(_, bcPivots.value, bcSubspaces.value))

    require(numberOfPartitions == subspaces.length, "Something has gone very wrong. Number of partitions does not match number of subspaces.")
    val partitionedRDD = data.partitionBy(new HashPartitioner(numberOfPartitions)).map(_._2)

    val clusteredRDD: RDD[DataPoint] = partitionedRDD.mapPartitions(iter => {
      val partition = iter.toArray
      val rng = new scala.util.Random(seed + TaskContext.getPartitionId())
      val dimension = rng.nextInt(partition.head.dimensions)

      val sortedPartition = partition.sortBy(point => point.vectorRep(dimension))
      val neighbourhoods = SWNQA(sortedPartition, dimension, epsilon, minPts)

      localDBSCAN(sortedPartition, neighbourhoods, minPts).iterator
    })


    // As per DBSCAN-MS, Section VII: "we only need to transfer the core and border objects in the margins"
    val mergingCandidates = clusteredRDD.filter(point =>
      (point.mask == MASK.MARGIN_OUTER || point.mask == MASK.MARGIN_INNER) &&
        (point.label == LABEL.CORE || point.label == LABEL.BORDER)).collect()
    val bcGlobalClusterMappings = sc.broadcast(CCGMA(mergingCandidates))

    // Filter duplicates and
    // Merge local clusters into global clusters. If one cluster sits entirely in one partition,
    // we give it a unique ID by encoding the partition number in the high bits.
    val bitOffset = 19
    clusteredRDD.filter(_.mask != MASK.MARGIN_OUTER).map(point => {
      bcGlobalClusterMappings.value.get((point.partition, point.localCluster)) match {
        case Some(cluster) => point.globalCluster = cluster
        case None => point.globalCluster = if (point.localCluster == -1) -1 else {
          ((point.partition + 1) << bitOffset) | point.localCluster
        }
      }
      point
    })
  }

  /**
   * Reads a dataset of numeric vectors from a text or CSV file into an RDD of [[DataPoint]] objects.
   *
   * Each line of the input file is expected to represent a single data point as a comma-separated list
   * of numeric values. Optionally, a header row and/or a ground-truth label column may be present.
   * Line indices are preserved and assigned as unique IDs to each [[DataPoint]].
   *
   * @param sc            The active [[SparkContext]] used for file reading and RDD creation.
   * @param path          Path to the input dataset file.
   * @param hasHeader     Whether the input file contains a header row (default: `false`).
   * @param hasRightLabel Whether the input file contains a ground-truth label in the last column (default: `false`).
   * @return An RDD of [[DataPoint]] objects representing the parsed dataset.
   */
  def readData(sc: SparkContext, path: String, hasHeader: Boolean, hasRightLabel: Boolean): RDD[DataPoint] = {
    val rdd = sc.textFile(path).zipWithIndex()
    val rdd1 = if (hasHeader) rdd.filter(_._2 > 0) else rdd
    rdd1.map { case (line, index) => makeDataPoint(line, index, hasRightLabel) }
  }

  private def makeDataPoint(line: String, index: Long, hasRightLabel: Boolean = false): DataPoint = {
    val data = line.split(",").map(_.toFloat)
    val cleanedData = if (hasRightLabel) data.dropRight(1) else data
    DataPoint(cleanedData, id = index)
  }

  private def estimatePivotNumber(rdd: RDD[DataPoint],
                                  samplingDensity: Double,
                                  seed: Int): Int = {
    Console.out.println("Warning. Estimating the number of pivots may take a while on large samples.")
    val sampledData = rdd.sample(withReplacement = false, fraction = samplingDensity, seed = seed).collect()
    val intrDim = IntrinsicDimensionality(sampledData)

    // Based on 'DBSCAN-MS: Distributed Density-Based Clustering in Metric Spaces' Section VIII.C
    // "well-chosen pivots should be 1 to 2 times the intrinsic dimensionality of the dataset."
    val pivots = Math.ceil(intrDim * 1.75).toInt
    Console.out.println(f"Chosen number of pivots: $pivots")
    pivots
  }
}
