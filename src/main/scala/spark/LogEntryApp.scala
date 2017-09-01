package spark

import org.apache.spark.sql.SparkSession

object LogEntryApp extends App {
  val sparkSession = SparkSession.builder
    .master("local[2]")
    .appName("sparky")
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext

  run()

  def run(): Unit = {
  }
}