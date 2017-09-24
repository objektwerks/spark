package spark

import org.apache.spark.sql.SparkSession

import scala.io.Source

object SparkInstance {
  val sparkSession = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val licenseText = Source.fromInputStream(getClass.getResourceAsStream("/license.mit")).getLines.toSeq
  println("Initialized spark instance.")

  sys.addShutdownHook {
    sparkSession.stop()
    println("Terminated spark instance.")
  }
}