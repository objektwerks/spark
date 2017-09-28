package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{FunSuite, Matchers}

class DataSourceTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("text") {
    val lines: RDD[String] = sparkContext.textFile("./data/txt/license.txt")
    lines.count shouldBe 19
  }

  test("json") {
    val dataset: Dataset[Person] = sparkSession.read.json("./data/json/person.json").as[Person]
    dataset.count shouldBe 4

    val dataframe: Dataset[Row] = sparkSession.read.json("./data/json/person.json")
    dataframe.count shouldBe 4
  }
}