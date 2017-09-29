package spark

import org.apache.spark.sql.SparkSession

object SparkInstance {
  val sparkSession = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  println("Initialized spark instance.")

  sys.addShutdownHook {
    sparkSession.stop()
    println("Terminated spark instance.")
  }
}