package testutils

import org.apache.spark.sql.SparkSession

object TestSparkSession {
  private def newSession(): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkSession.builder()
      .appName("DBSCAN-MS Test")
      .master("local[14]") // * for all cores
      .config("spark.local.dir", "S:\\temp")
      .config("spark.driver.memory", "4g")
      .config("spark.driver.maxResultSize", "15g")
      .config("spark.executor.memory", "8g")
      .getOrCreate()
  }

  def getOrCreate(): SparkSession = synchronized {
    SparkSession.getActiveSession match {
      case Some(s) if !s.sparkContext.isStopped =>
        s
      case Some(s) =>
        safeStop(s)
        newSession()
      case None =>
        newSession()
    }
  }

  private def safeStop(spark: SparkSession): Unit = {
    try spark.stop()
    catch { case _: Throwable => () }
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }
}
