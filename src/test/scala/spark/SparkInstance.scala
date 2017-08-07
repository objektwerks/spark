package spark

import org.apache.spark.sql.SparkSession

import scala.io.Source

object SparkInstance {
  val sparkSession = SparkSession.builder
    .master("local[2]")
    .appName("sparky")
    .getOrCreate()
  val personJson = Source.fromInputStream(getClass.getResourceAsStream("/person.json.txt")).getLines.toSeq
  val dataJson = Source.fromInputStream(getClass.getResourceAsStream("/data.json.txt")).getLines.toSeq
  val license = Source.fromInputStream(getClass.getResourceAsStream("/license.mit")).getLines.toSeq
}