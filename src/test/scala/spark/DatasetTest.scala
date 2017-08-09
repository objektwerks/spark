package spark

import org.scalatest.{FunSuite, Matchers}

class DatasetTest extends FunSuite with Matchers {
  test("dataset") {
    import SparkInstance._
    import sparkSession.implicits._

    val dataset = sparkSession.read.json(personJson.toDS()).as[Person]

    dataset.count shouldBe 4
    dataset.filter(_.age == 24).first.name shouldBe "fred"

    val names = dataset.select("name").orderBy("name").collect
    names.length shouldBe 4
    names.head.mkString shouldBe "barney"

    val ages = dataset.select("age").orderBy("age").collect
    ages.length shouldBe 4
    ages.head.getLong(0) shouldBe 21
  }
}