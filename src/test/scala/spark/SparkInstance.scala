package spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object SparkInstance {
  val conf = new SparkConf().setMaster("local[2]").setAppName("sparky")
  val context = new SparkContext(conf)
  val sqlContext = new SQLContext(context)
  val json = Source.fromInputStream(getClass.getResourceAsStream("/spark.json.txt")).getLines.toSeq
  val license = Source.fromInputStream(getClass.getResourceAsStream("/license.mit")).getLines.toSeq
}