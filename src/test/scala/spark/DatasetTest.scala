package spark

import org.scalatest.{FunSuite, Matchers}

class DatasetTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._
  import Person._

  val dataset = sparkSession.read.json("./data/json/person.json").as[Person].cache

  test("dataset") {
    dataset.count shouldBe 4

    val filterPersonByName = dataset.filter(_.name == "barney")
    filterPersonByName.count shouldBe 1
    filterPersonByName.head.name shouldBe "barney"

    val filterPersonByAge = dataset.filter(_.age > 23)
    filterPersonByAge.count shouldBe 1
    filterPersonByAge.head.age shouldBe 24

    val selectNameByAge = dataset.select("name").where("age == 24").as[String]
    selectNameByAge.count shouldBe 1
    selectNameByAge.head shouldBe "fred"

    dataset.map(_.age).collect.min shouldBe 21
    dataset.map(_.age).collect.avg shouldBe 22.5
    dataset.map(_.age).collect.max shouldBe 24
    dataset.map(_.age).collect.sum shouldBe 90
  }

  test("dataframe") {
    val dataframe = dataset.toDF.cache
    dataframe.count shouldBe 4

    dataframe.agg(Map("age" -> "min")).first.getLong(0) shouldBe 21
    dataframe.agg(Map("age" -> "avg")).first.getDouble(0) shouldBe 22.5
    dataframe.agg(Map("age" -> "max")).first.getLong(0) shouldBe 24
    dataframe.agg(Map("age" -> "sum")).first.getLong(0) shouldBe 90
  }

  test("sql") {
    dataset.createOrReplaceTempView("persons")
    val persons = dataset.sqlContext.sql("select * from persons where age >= 21 and age <= 24 order by age").as[Person].cache
    persons.count shouldBe 4
    persons.head.name shouldBe "betty"
    persons.head.age shouldBe 21
  }
}