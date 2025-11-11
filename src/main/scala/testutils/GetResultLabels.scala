package testutils

import model.DataPoint
import org.apache.log4j.Logger

object GetResultLabels {
  private val log = org.apache.log4j.LogManager.getLogger(GetResultLabels.getClass)

  /**
   * Extracts the global cluster labels from a given array of clustered [[DataPoint]]s,
   * optionally reorders them to match the original dataset, remaps them to a contiguous
   * zero-based label range, and/or sorts them numerically.
   *
   * The function supports three mutually exclusive modes of output ordering:
   *   - Default: Keeps the order of `result` as provided.
   *   - Original dataset order: If `originalDataset` is defined, output labels are reordered
   *     to match the row order of the original input data.
   *   - Sorted order: If `sorted` is `true`, output labels are sorted numerically.
   *
   * @param result           The clustered dataset as an array of [[DataPoint]]s, each containing
   *                         a global cluster assignment.
   * @param remappedLabels   Whether to remap cluster labels into a contiguous 0-based range.
   *                         Noise points (`-1`) are preserved as `-1`.
   *                         Default is `true`.
   * @param originalDataset  Optional original dataset as an array of numeric vectors.
   *                         If defined, the function will reorder the labels to match the order
   *                         of this dataset based on hash codes of the data vectors.
   * @param sorted           Whether to return the labels sorted in ascending order.
   *                         Mutually exclusive with `originalDataset`.
   * @return                 An array of integer cluster labels corresponding to the clustered points,
   *                         ordered and remapped according to the provided parameters.
   */
  def apply(result: Array[DataPoint],
            remappedLabels: Boolean = true,
            originalDataset: Option[Array[Array[Float]]] = None,
            sorted: Boolean = false): Array[Int] = {
    require(!sorted || (originalDataset match {
      case None => true
      case _ => false}), "Labels can only be ordered according to either the original dataset, or sorted, not both")

    val r: Array[DataPoint] = if (originalDataset.isDefined) {
      val orderMap = originalDataset.get.map(java.util.Arrays.hashCode).zipWithIndex.toMap
      val indexedResult = result.map(p => (p, orderMap(java.util.Arrays.hashCode(p.data))))
      val sortedResult = indexedResult.sortBy(_._2)
      sortedResult.map(_._1)
    } else result


    val rawLabels = r.map(_.globalCluster)

    val labels = if (remappedLabels) {
      val newLabelMapping = rawLabels.distinct.filterNot(_ == -1).zipWithIndex.toMap
      rawLabels.map(l => if (l == -1) -1 else newLabelMapping(l))
    } else rawLabels

    if (sorted) labels.sorted else labels
  }

  /**
   * Prints a summary of the cluster distribution in the given result set.
   *
   * This function computes the predicted cluster labels from the provided
   * array of [[DataPoint]]s, groups the points by
   * their assigned cluster ID, and prints the size of each cluster.
   * Noise points (label `-1`) are included in the summary.
   *
   * @param result The clustered dataset as an array of [[DataPoint]]s.
   */
  def printClusters(result: Array[DataPoint]): Unit = {
    val predLabels = GetResultLabels(result)
    val groupedLabels = predLabels.groupBy(identity)
    groupedLabels.foreach(x => log.debug(s"Cluster ${x._1} has ${x._2.length} points"))
    log.info(s"Total clusters (including noise): ${groupedLabels.size}")
  }
}
