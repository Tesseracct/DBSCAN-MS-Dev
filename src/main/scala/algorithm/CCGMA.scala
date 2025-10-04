package algorithm

import model.{DataPoint, LABEL}
import org.jgrapht.alg.connectivity.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import scala.jdk.CollectionConverters.{ListHasAsScala, SetHasAsScala}

//TODO: Consider optimization here, how expensive is asScala?
object CCGMA {
  /**
   * Applies the CCGMA merging algorithm to the merging candidates.
   * The algorithm constructs a graph where local clusters are vertices and edges
   * connect clusters that share core points. It then identifies connected components
   * in the graph to assign global cluster IDs.
   *
   * @param mergingCandidates An array of DataPoint objects to be merged into global clusters.
   * @return A map where keys are tuples of (partition, localCluster) and values are the corresponding global cluster IDs.
   */
  def apply(mergingCandidates: Array[DataPoint]): Map[(Int, Int), Int] = {
    execute(mergingCandidates)
  }

  def execute(mergingCandidates: Array[DataPoint]): Map[(Int, Int), Int] = {
    val mergingObjects: Map[Long, Array[DataPoint]] = mergingCandidates.groupBy(_.id)

    val graph = new SimpleGraph[(Int, Int), DefaultEdge](classOf[DefaultEdge])
    for ((_, mObjects) <- mergingObjects) {
      val localClusterToMergingObject = mObjects.map(x => ((x.partition, x.localCluster), x)).toMap
      val localResults = localClusterToMergingObject.keys.toArray.distinct
      if (localResults.length >= 2) {
        for (i <- localResults.indices; j <- i + 1 until localResults.length) {
          val c1 = localResults(i)
          val c2 = localResults(j)

          // This is technically unsafe, but if the Map returns null something catastrophic has failed anyway.
          if (localClusterToMergingObject.getOrElse(c1, null).label == LABEL.CORE
            || localClusterToMergingObject.getOrElse(c2, null).label == LABEL.CORE) {
            graph.addVertex(c1)
            graph.addVertex(c2)
            graph.addEdge(c1, c2)
          }
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
