package model

case class DataPointVector(coordinates: Array[Float], id: Long, var label: Int = LABEL.NOISE, var visited: Boolean = false) {
  override def equals(obj: Any): Boolean = obj match {
    case that: DataPointVector => this.coordinates sameElements that.coordinates
    case _ => false
  }

  override def hashCode(): Int = coordinates.hashCode()

  override def toString: String = s"DataPoint(${coordinates.mkString(", ")}, id=$id, label=$label, visited=$visited)"

}
