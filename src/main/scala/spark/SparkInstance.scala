package spark

import java.net.InetAddress

import org.apache.spark.sql.SparkSession

object SparkInstance {
  val sparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName(InetAddress.getLocalHost.getHostName)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.warehouse.dir", "./target/spark-warehouse")
    .config("spark.eventLog.enabled", true)
    .config("spark.eventLog.dir", "./target")
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  sparkContext.addSparkListener(SparkAppListener())
  println("Initialized Spark instance.")

  sys.addShutdownHook {
    sparkSession.stop()
    println("Terminated Spark instance.")
  }
}