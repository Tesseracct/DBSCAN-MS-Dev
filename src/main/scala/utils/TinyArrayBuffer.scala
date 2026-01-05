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
      // Use 1.5x growth for larger arrays to reduce memory pressure and fragmentation
      var newCapacity = if (elems.length < 1024) elems.length * 2 else elems.length + (elems.length >> 1)
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

  /**
   * Clears all elements from the buffer and releases memory.
   */
  def clear(): Unit = {
    elems = new Array[Int](4)  // Reset to minimal capacity
    elemCounter = 0
  }

  /**
   * Trims the internal array to the exact size needed.
   * Call this when no more elements will be added to free unused memory.
   */
  def trimToSize(): Unit = {
    if (elemCounter < elems.length) {
      val newArray = new Array[Int](math.max(elemCounter, 1))
      if (elemCounter > 0) System.arraycopy(elems, 0, newArray, 0, elemCounter)
      elems = newArray
    }
  }
}
