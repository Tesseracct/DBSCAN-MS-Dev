package algorithm

import model.{DataPoint, LABEL}

import scala.collection.mutable

object localDBSCAN {
  /**
   * Performs local DBSCAN clustering on the given dataset.
   * @param points The dataset to cluster.
   * @param neighbourhoods The neighbourhoods of each data point as per SWNQA.
   * @param minPts The minimum number of neighbours for a core point.
   * @return The clustered dataset.
   */
  def apply(points: Array[DataPoint], neighbourhoods: Array[Array[Int]], minPts: Int): Array[DataPoint] = {
    execute(points, neighbourhoods, minPts)
  }

  def execute(points: Array[DataPoint], neighbourhoods: Array[Array[Int]], minPts: Int): Array[DataPoint] = {
    var currentCluster = 0
    for (i <- points.indices) {
      val point = points(i)
      if (!point.visited) {
        point.visited = true

        val neighbourhood = neighbourhoods(i)
        if (neighbourhood.length + 1 >= minPts) {
          currentCluster += 1
          point.localCluster = currentCluster
          point.label = LABEL.CORE

          val queue = mutable.Queue[Int]()
          for (p <- neighbourhood) if (!points(p).visited || points(p).label == LABEL.NOISE) queue.enqueue(p)
          while (queue.nonEmpty) {
            val currentIndex = queue.dequeue()
            val currentPoint = points(currentIndex)
            currentPoint.localCluster = currentCluster

            if (!currentPoint.visited) {
              currentPoint.visited = true
              val currentNeighbourhood = neighbourhoods(currentIndex)
              if (currentNeighbourhood.length + 1 >= minPts) {
                currentPoint.label = LABEL.CORE
                for (p <- currentNeighbourhood) if (!points(p).visited || points(p).label == LABEL.NOISE) queue.enqueue(p)
              } else {
                currentPoint.label = LABEL.BORDER
              }
            } else {
              if (currentPoint.label == LABEL.NOISE) {
                currentPoint.label = LABEL.BORDER
              }
            }
          }
        }
      }
    }
    points
  }
}
