package utils

private trait Distance[T] extends Serializable {
  def distance(a: T, b: T): Float
}

case object Distance {
  /* Implementation is too memory-inefficient
  def euclidean(a: Array[Float], b: Array[Float]): Float = {
    require(a.length == b.length, "Vectors must be of the same length")
    math.sqrt(a.zip(b).map { case (x, y) => math.pow(x - y, 2) }.sum).toFloat
  }
  */

  final def euclidean(a: Array[Float], b: Array[Float]): Float = {
    require(a.length == b.length, "Vectors must be of the same length")
    var i = 0
    var acc = 0.0f
    val n = a.length
    while (i < n) {
      val d = a(i) - b(i)
      acc += d * d
      i += 1
    }
    math.sqrt(acc).toFloat
  }



}
