package model

case class DataPoint(data: Array[Float], id: Long, var label: Int = LABEL.UNDEFINED, var visited: Boolean = false, var vectorRep: Option[Array[Float]] = None) {
  override def equals(obj: Any): Boolean = obj match {
    case that: DataPointVector => this.data sameElements that.coordinates
    case _ => false
  }

  override def hashCode(): Int = data.hashCode()

  override def toString: String = s"DataPoint(${data.mkString(", ")}, id=$id, label=$label, visited=$visited)"

  def distance(other: DataPoint, distanceFunction: (Array[Float], Array[Float]) => Float): Float = {
    distanceFunction(this.data, other.data)
  }

}
