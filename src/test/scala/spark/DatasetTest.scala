package spark

import org.scalatest.{FunSuite, Matchers}

class DatasetTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  val dataset = sparkSession.read.json(personJson.toDS()).as[Person].cache

  test("dataset") {
    val personsByName = dataset.filter(_.name == "barney").as[Person]
    personsByName.count shouldBe 1
    personsByName.head.name shouldBe "barney"

    val personsByAge = dataset.filter(_.age > 23).as[Person]
    personsByAge.count shouldBe 1
    personsByAge.head.age shouldBe 24
  }

  test("dataframe") {
    val minAgeAsRow = dataset.agg(Map("age" -> "min")).first
    minAgeAsRow.getLong(0) shouldBe 21

    val avgAgeAsRow = dataset.agg(Map("age" -> "avg")).first
    avgAgeAsRow.getDouble(0) shouldBe 22.5

    dataset.createOrReplaceTempView("persons")
    val personsAsDataframe = dataset.sqlContext.sql("select * from persons where age >= 21 and age <= 24 order by age").cache
    personsAsDataframe.count shouldBe 4
    personsAsDataframe.head.getString(1) shouldBe "betty"
  }

  test("sql") {
    dataset.createOrReplaceTempView("persons")
    val persons = dataset.sqlContext.sql("select * from persons where age >= 21 and age <= 24 order by age").as[Person].cache
    persons.count shouldBe 4
    persons.head.name shouldBe "betty"
    persons.head.age shouldBe 21
  }
}