package algorithm

import model.{DataPoint, LABEL}
import org.jgrapht.alg.connectivity.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import scala.jdk.CollectionConverters.{ListHasAsScala, SetHasAsScala}

object CCGMA {
  /**
   * Applies the CCGMA merging algorithm to the merging candidates.
   * The algorithm constructs a graph where local clusters are vertices and edges
   * connect clusters that share core points. It then identifies connected components
   * in the graph to assign global cluster IDs.
   *
   * @param mergingCandidates An array of DataPoint[A] objects to be merged into global clusters.
   * @return A map where keys are tuples of (partition, localCluster) and values are the corresponding global cluster IDs.
   */
  def apply[A](mergingCandidates: Array[DataPoint[A]]): Map[(Int, Int), Int] = {
    execute(mergingCandidates)
  }

  def execute[A](mergingCandidates: Array[DataPoint[A]]): Map[(Int, Int), Int] = {
    val mergingObjects: Map[Long, Array[DataPoint[A]]] = mergingCandidates.groupBy(_.id)

    val graph = new SimpleGraph[(Int, Int), DefaultEdge](classOf[DefaultEdge])
    for ((_, mObjects) <- mergingObjects) {
      for (i <- mObjects.indices; j <- i + 1 until mObjects.length) {
        val io = mObjects(i)
        val jo = mObjects(j)

        if (io.label == LABEL.CORE || jo.label == LABEL.CORE){
          val c1 = (io.partition, io.localCluster)
          val c2 = (jo.partition, jo.localCluster)

          graph.addVertex(c1)
          graph.addVertex(c2)
          graph.addEdge(c1, c2)
        }
      }
    }
    val connectedComponents = new ConnectivityInspector[(Int, Int), DefaultEdge](graph).connectedSets()

    connectedComponents.asScala.zipWithIndex.flatMap { case (globalCluster, index) =>
        val globalClusterID = index + 1
        globalCluster.asScala.map((_, globalClusterID))
      }.toMap
  }
}
