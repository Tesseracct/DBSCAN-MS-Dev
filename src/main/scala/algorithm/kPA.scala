package algorithm

import breeze.linalg.DenseVector
import model.{DataPoint, Subspace}
import utils.MapPointToVectorSpace

object kPA {
  def apply(dataset: Array[DataPoint], pivots: Array[DataPoint], subspaces: Array[Subspace]): Unit = {
    dataset.foreach {point => {
      if (point.vectorRep == null) point.vectorRep = MapPointToVectorSpace(point, pivots)
    }}









  }


  def computeOuterSubspace(subspace: Subspace, epsilon: Float): Subspace = {
    ???
  }

  def computeInnerSubspace(subspace: (Array[Float], Array[Float]), epsilon: Float): (Array[Float], Array[Float]) = {
    val (minCoords, maxCoords) = subspace
    val contractedMin = minCoords.map(_ + epsilon)
    val contractedMax = maxCoords.map(_ - epsilon)
    (contractedMin, contractedMax)
  }

  def computeOuterMargin(subspace: (Array[Float], Array[Float]),
                         outerSubspace: (Array[Float], Array[Float])):
                        Unit = {

    val (minCoords, maxCoords) = subspace
    val (outerMin, outerMax) = outerSubspace


  }
}
