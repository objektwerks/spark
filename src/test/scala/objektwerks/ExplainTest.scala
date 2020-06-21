package objektwerks

import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExplainTest extends AnyFunSuite with Matchers {
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