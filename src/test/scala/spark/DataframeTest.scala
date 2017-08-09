package spark

import org.scalatest.{FunSuite, Matchers}

class DataframeTest extends FunSuite with Matchers {
  test("dataframe") {
    import SparkInstance._
    import sparkSession.implicits._

    val dataframe = sparkSession.read.json(personJson.toDS()).as[Person]

    val names = dataframe.select("name").orderBy("name").collect
    names.length shouldBe 4
    names.head.mkString shouldBe "barney"

    val ages = dataframe.select("age").orderBy("age").collect
    ages.length shouldBe 4
    ages.head.getLong(0) shouldBe 21

    val fred = dataframe.filter(dataframe("age") > 23).first
    fred.age shouldBe 24
    fred.name shouldBe "fred"

    val minAge = dataframe.agg(Map("age" -> "min")).first
    minAge.getLong(0) shouldBe 21

    val avgAge = dataframe.agg(Map("age" -> "avg")).first
    avgAge.getDouble(0) shouldBe 22.5
  }
}