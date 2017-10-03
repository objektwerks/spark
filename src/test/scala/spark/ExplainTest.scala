package spark

import org.scalatest.{FunSuite, Matchers}

class ExplainTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  val dataset = sparkSession.read.json("./data/json/person.json").as[Person].cache

  test("explain") {
    val filterByName = dataset.map(_.name.toUpperCase).filter(_ == "FRED").cache
    filterByName.explain(extended = true)
    filterByName.count shouldBe 1
    filterByName.head shouldBe "FRED"
  }
}