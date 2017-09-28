package spark

import org.apache.spark.rdd.RDD
import org.scalatest.{FunSuite, Matchers}

class DataSourceTest extends FunSuite with Matchers {
  import SparkInstance._

  test("text") {
    val lines: RDD[String] = sparkContext.textFile("./data/txt/license.txt")
    lines.count shouldBe 19
  }
}