package algorithm

import breeze.linalg.DenseVector
import model.DataPoint
import utils.MapPointToVectorSpace

object kPA {
  def apply(dataset: Array[DataPoint], pivots: Array[DataPoint], subspaces: Array[(Array[Float], Array[Float])]): Unit = {
    dataset.foreach {point => {
      if (point.vectorRep == null) point.vectorRep = MapPointToVectorSpace(point, pivots)
    }}









  }


  def computeOuterSubspace(subspace: (Array[Float], Array[Float]), epsilon: Float): (Array[Float], Array[Float]) = {
    val (minCoords, maxCoords) = subspace
    val expandedMin = minCoords.map(_ - epsilon)
    val expandedMax = maxCoords.map(_ + epsilon)
    (expandedMin, expandedMax)
  }

  def computeInnerSubspace(subspace: (Array[Float], Array[Float]), epsilon: Float): (Array[Float], Array[Float]) = {
    val (minCoords, maxCoords) = subspace
    val contractedMin = minCoords.map(_ + epsilon)
    val contractedMax = maxCoords.map(_ - epsilon)
    (contractedMin, contractedMax)
  }

  def computeOuterMargin(subspace: (Array[Float], Array[Float]),
                         outerSubspace: (Array[Float], Array[Float])):
                        ((Array[Float], Array[Float]), (Array[Float], Array[Float])) = {

    val (minCoords, maxCoords) = subspace
    val (outerMin, outerMax) = outerSubspace


  }
}
