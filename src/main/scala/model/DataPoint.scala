package model

import java.util


case class DataPoint(data: Array[Float],
                     id: Long,
                     var label: Int = LABEL.UNDEFINED,
                     var visited: Boolean = false,
                     var vectorRep: Array[Float] = null,
                     var mask: Int = -1,
                     var localCluster: Int = -1,
                     var partition: Int = -1,
                     var globalCluster: Int = -1) {
  override def equals(obj: Any): Boolean = obj match {
    case that: DataPoint => this.id == that.id && this.partition == that.partition && this.data == that.data
    case _ => false
  }

  override def hashCode(): Int = {
    val prime = 31
    prime * (prime * (prime + util.Arrays.hashCode(data)) + partition) + id.toInt
  }

  override def toString: String = s"DataPoint(${data.mkString(", ")}, id=$id, label=$label, visited=$visited, " +
                                  s"vectorRep=${if (vectorRep != null) vectorRep.mkString(", ")}, " +
                                  s"mask=$mask, cluster=$localCluster, partition=$partition, globalCluster=$globalCluster)"

  def distance(other: DataPoint, distanceFunction: (Array[Float], Array[Float]) => Float): Float = {
    distanceFunction(this.data, other.data)
  }

  /**
   * @return The number of dimensions of vectorRep.
   */
  def dimensions: Int = vectorRep.length

  /**
   * Copies the DataPoint and sets the vectorRep to the given value.
   * @param vectorRep The new vector representation.
   * @return A new DataPoint with the given vectorRep.
   *
   * @note We're only making shallow copies, therefore assuming data won't be changed after withVectorRep is called
   */
  def withVectorRep(vectorRep: Array[Float]): DataPoint = this.copy(vectorRep = vectorRep)

  /**
   * Copies the DataPoint and sets the mask to the given value.
   * @param mask The new mask.
   * @return A new DataPoint with the given mask.
   *
   * @note We're only making shallow copies, therefore assuming data & vectorRep won't be changed after withMask is called
   */
  def withMask(mask: Int): DataPoint = this.copy(mask = mask)
}
