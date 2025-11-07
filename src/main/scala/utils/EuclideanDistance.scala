package utils


object EuclideanDistance {
  final def distance(a: Array[Float], b: Array[Float]): Float = {
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


