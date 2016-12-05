package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object SparkInstance {
  val conf = new SparkConf().setMaster("local[2]").setAppName("sparky")
  val context = new SparkContext(conf)
  val session = SparkSession.builder().getOrCreate()
  val personJson = Source.fromInputStream(getClass.getResourceAsStream("/person.json.txt")).getLines.toSeq
  val dataJson = Source.fromInputStream(getClass.getResourceAsStream("/data.json.txt")).getLines.toSeq
  val license = Source.fromInputStream(getClass.getResourceAsStream("/license.mit")).getLines.toSeq
}

case class Data(n: Long)

case class Person(age: Long, name: String)