package utils

import model.DataPoint

object GetResultLabels {
  def apply(result: Array[DataPoint],
            remappedLabels: Boolean = true,
            originalDataset: Option[Array[Array[Float]]] = None,
            sorted: Boolean = false): Array[Int] = {
    require(!sorted || (originalDataset match {
      case None => true
      case _ => false}), "Labels can only be ordered according to either the original dataset, or sorted, not both")

    val r: Array[DataPoint] = if (originalDataset.isDefined) {
      val orderMap = originalDataset.get.zipWithIndex.toMap
      val indexedResult = result.map(p => (p, orderMap(p.data)))
      val sortedResult = indexedResult.sortBy(_._2)
      sortedResult.map(_._1)
    } else result

    val points = r.map(p => (p.id, p.globalCluster)).distinct
    val rawLabels = points.map(_._2.toInt) // TODO: Remove toInt cast after fixing bitshift

    val labels = if (remappedLabels) {
      val newLabelMapping = rawLabels.distinct.filterNot(_ == -1).zipWithIndex.toMap
      rawLabels.map(l => if (l == -1) -1 else newLabelMapping(l))
    } else rawLabels

    if (sorted) labels.sorted else labels
  }

}
