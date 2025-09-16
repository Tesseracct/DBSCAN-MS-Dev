package algorithm

import model.{DataPoint, LABEL}
import org.jgrapht.alg.connectivity.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import scala.jdk.CollectionConverters.{ListHasAsScala, SetHasAsScala}

case object CCGMA {
  def apply(mergingCandidates: Array[DataPoint]): Map[(Int, Int), Long] = {
    val localResults: Map[Long, Array[DataPoint]] = mergingCandidates.groupBy(_.id)

    val graph = new SimpleGraph[DataPoint, DefaultEdge](classOf[DefaultEdge])
    for ((_, localClusters) <- localResults) {
      if (localClusters.length >= 2) {
        for (i <- localClusters.indices; j <- i + 1 until localClusters.length) {
          val p1 = localClusters(i)
          val p2 = localClusters(j)

          if (p1.label == LABEL.CORE || p2.label == LABEL.CORE) {
            graph.addVertex(p1)
            graph.addVertex(p2)
            graph.addEdge(p1, p2)
          }
        }
      }
    }
    val connectedComponents = new ConnectivityInspector[DataPoint, DefaultEdge](graph).connectedSets()

    connectedComponents.asScala.zipWithIndex.flatMap { case (globalCluster, index) =>
        val globalClusterID = index + 1
        globalCluster.asScala.map { partialCluster =>
          ((partialCluster.partition, partialCluster.localCluster), globalClusterID.toLong)
        }
      }.toMap
  }
}
