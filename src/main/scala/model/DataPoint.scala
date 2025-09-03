package model

case class DataPoint(data: Array[Float], id: Long, var label: Int = LABEL.UNDEFINED, var visited: Boolean = false, var vectorRep: Array[Float] = null) {
  override def equals(obj: Any): Boolean = obj match {
    case that: DataPoint => this.data sameElements that.data
    case _ => false
  }

  override def hashCode(): Int = data.hashCode()

  override def toString: String = s"DataPoint(${data.mkString(", ")}, id=$id, label=$label, visited=$visited)"

  def distance(other: DataPoint, distanceFunction: (Array[Float], Array[Float]) => Float): Float = {
    distanceFunction(this.data, other.data)
  }

}
