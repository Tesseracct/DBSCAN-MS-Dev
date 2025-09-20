package utils

import scala.io.Source
import scala.util.Using

case object Testing {
  /**
   * Read the data from a CSV file into an array of arrays.
   * @param filePath The path to the CSV file.
   * @return An array of arrays containing the data.
   */
  def readDataToString(filePath: String, header: Boolean): Array[Array[String]] = {
    val x = Using(Source.fromFile(filePath)) { source =>
      source.getLines().map(_.split(',')).toArray
    }.get
    if (header) x.tail else x
  }

  /**
   * Split the data into two arrays: one containing the features and the other containing the labels.
   * @param data The data to split.
   * @return A tuple containing the features and labels as arrays.
   */
  def splitData(data: Array[Array[String]]): (Array[Array[Float]], Array[Int]) = {
    data.map(row => (row.init.map(_.toFloat), row.last.toInt)).unzip
  }

  def getRightmostColumn(filePath: String): Seq[String] = {
    Using(Source.fromFile(filePath)) { source =>
      source.getLines().drop(1).map(_.split(',').last).toSeq
    }.getOrElse(Seq.empty)
  }


}
