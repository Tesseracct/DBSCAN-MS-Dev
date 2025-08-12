package model

case class DataPoint(coordinates: Array[Float], id: Long, var label: Int = LABEL.UNDEFINED, var visited: Boolean = false, var distances: Array[Float] = null) {
  override def equals(obj: Any): Boolean = obj match {
    case that: DataPoint => this.coordinates sameElements that.coordinates
    case _ => false
  }

  override def hashCode(): Int = coordinates.hashCode()

  override def toString: String = s"DataPoint(${coordinates.mkString(", ")}, id=$id, label=$label, visited=$visited)"

}
