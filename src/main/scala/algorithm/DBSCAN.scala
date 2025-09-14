package algorithm

import model.{DataPoint, LABEL}

import scala.collection.mutable

case object DBSCAN {
  def apply(points: Array[DataPoint], neighbourhoods: Array[Array[Int]], minPts: Int): Array[DataPoint] = {
    var currentCluster = 0
    for (i <- points.indices) {
      val point = points(i)
      if (!point.visited) {
        point.visited = true
        point.label = LABEL.NOISE

        val neighbourhood = neighbourhoods(i)
        if (neighbourhood.length >= minPts) {
          currentCluster += 1
          point.cluster = currentCluster
          point.label = LABEL.CORE

          val queue = mutable.Queue[Int]().enqueueAll(neighbourhood)
          while (queue.nonEmpty) {
            val currentIndex = queue.dequeue()
            val currentPoint = points(currentIndex)
            if (!currentPoint.visited) {
              currentPoint.visited = true
              currentPoint.cluster = currentCluster

              val currentNeighbourhood = neighbourhoods(currentIndex)
              if (currentNeighbourhood.length >= minPts) {
                currentPoint.label = LABEL.CORE
                queue.enqueueAll(currentNeighbourhood)
              } else {
                currentPoint.label = LABEL.BORDER
              }
            } else {
              currentPoint.cluster = currentCluster
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
