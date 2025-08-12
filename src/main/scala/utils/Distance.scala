package utils

case object Distance {
  def euclidean(a: Array[Float], b: Array[Float]): Float = {
    require(a.length == b.length, "Vectors must be of the same length")
    math.sqrt(a.zip(b).map { case (x, y) => math.pow(x - y, 2) }.sum).toFloat
  }
}
