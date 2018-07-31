package spark

import org.apache.spark.sql.Dataset
import org.scalatest.{FunSuite, Matchers}

class ExplainTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("explain") {
    val persons: Dataset[Person] = sparkSession.read.json("./data/person/person.json").as[Person].cache
    val fred: Dataset[String] = persons.map(_.name.toUpperCase).filter(_ == "FRED").cache
    fred.explain(extended = true)
    fred.count shouldBe 1
    fred.head shouldBe "FRED"
  }
}