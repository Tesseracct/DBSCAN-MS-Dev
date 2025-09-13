package algorithm

import model.DataPoint
import org.apache.spark.rdd.RDD

case object DBSCAN {
  def apply(objects: RDD[(DataPoint, List[DataPoint])], epsilon: Float): RDD[DataPoint] = ???
}
