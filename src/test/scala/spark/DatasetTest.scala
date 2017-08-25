package spark

import org.scalatest.{FunSuite, Matchers}

class DatasetTest extends FunSuite with Matchers {
  test("dataframe ~ dataset ~ sql") {
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

    val fred = dataset.filter(dataset("age") > 23).first
    fred.age shouldBe 24
    fred.name shouldBe "fred"

    val minAge = dataset.agg(Map("age" -> "min")).first
    minAge.getLong(0) shouldBe 21

    val avgAge = dataset.agg(Map("age" -> "avg")).first
    avgAge.getDouble(0) shouldBe 22.5

    dataset.createOrReplaceTempView("persons")
    val persons = sparkSession.sql("SELECT * FROM persons WHERE age >= 21 AND age <= 24").as[Person]
    persons.count shouldBe 4
  }
}