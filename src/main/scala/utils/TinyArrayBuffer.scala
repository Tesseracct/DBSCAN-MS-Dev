package utils

/**
 * A minimalistic, memory-efficient alternative to `scala.collection.mutable.ArrayBuffer`
 * that avoids boxing, reducing memory usage to roughly one quarter of the standard implementation.
 *
 * Only provides methods to append elements one at a time, returning length and converting to a plain `Array[Int]`.
 *
 * @param initialCapacity The initial capacity of the buffer. (default: 16).
 */
final class TinyArrayBuffer(initialCapacity: Int = 16) {
  private final val maxArraySize = Int.MaxValue - 8
  private var elems: Array[Int] = new Array[Int](initialCapacity)
  private var elemCounter: Int = 0

  private def ensureCapacity(minCapacity: Int): Unit = {
    if (minCapacity < 0) throw new RuntimeException("Overflow Error when trying to grow TinyArrayBuffer. (Exceeded Int.MaxValue)")
    if (minCapacity > maxArraySize) throw new RuntimeException("New capacity exceeds JVM array limit.")
    if (minCapacity > elems.length) {
      var newCapacity = elems.length * 2
      if (newCapacity < 0) newCapacity = maxArraySize
      if (newCapacity < minCapacity) newCapacity = minCapacity
      val newArray = new Array[Int](newCapacity)
      System.arraycopy(elems, 0, newArray, 0, elemCounter)
      elems = newArray
    }
  }

  /**
   * Appends an element to the end of the buffer.
   *
   * @param elem The element to append.
   */
  def +=(elem: Int): Unit = {
    ensureCapacity(elemCounter + 1)
    elems(elemCounter) = elem
    elemCounter += 1
  }

  /**
   * Converts the buffer to a plain `Array[Int]`.
   *
   * @return An array containing all elements in the buffer.
   */
  def toArray: Array[Int] = {
    if (elemCounter == elems.length) {
      elems
    } else {
      val result = new Array[Int](elemCounter)
      System.arraycopy(elems, 0, result, 0, elemCounter)
      result
    }
  }

  /**
   * Returns the number of elements in the buffer.
   *
   * @return The number of elements.
   */
  def length: Int = elemCounter
}
