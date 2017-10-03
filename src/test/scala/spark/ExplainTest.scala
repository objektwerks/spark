package spark

import org.scalatest.{FunSuite, Matchers}

class ExplainTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("explain") {
    val persons = sparkSession.read.json("./data/json/person.json").as[Person].cache
    val fred = persons.map(_.name.toUpperCase).filter(_ == "FRED").cache
    fred.explain(extended = true)
    fred.count shouldBe 1
    fred.head shouldBe "FRED"
  }
}