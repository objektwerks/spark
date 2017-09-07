package spark

import org.scalatest.{FunSuite, Matchers}

class DatasetTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  val dataset = sparkSession.read.json("./data/json/person.json").as[Person].cache

  test("dataset") {
    dataset.count shouldBe 4
    val ages = dataset.map(p => Age(p.age)).collect
    println(ages)

    val filterPersonByName = dataset.filter(_.name == "barney").as[Person]
    filterPersonByName.count shouldBe 1
    filterPersonByName.head.name shouldBe "barney"

    val filterPersonByAge = dataset.filter(_.age > 23).as[Person]
    filterPersonByAge.count shouldBe 1
    filterPersonByAge.head.age shouldBe 24

    val selectNameByAge = dataset.select("name").where("age == 24").as[String]
    selectNameByAge.count shouldBe 1
    selectNameByAge.head shouldBe "fred"

    val minAge = dataset.collect.map(p => Age(p.age)).min
    minAge.number shouldBe 21

    val avgAge = dataset.map(p => Age(p.age)).collect.avg
    avgAge.number shouldBe 22

    val maxAge = dataset.collect.map(p => Age(p.age)).max
    maxAge.number shouldBe 24

    val sumAge = dataset.map(p => Age(p.age)).collect.total
    sumAge.number shouldBe 90
  }

  test("dataframe") {
    val dataframe = dataset.toDF.cache
    dataframe.count shouldBe 4

    val minAge = dataframe.agg(Map("age" -> "min"))
    minAge.count shouldBe 1
    minAge.first.getLong(0) shouldBe 21

    val avgAge = dataframe.agg(Map("age" -> "avg"))
    avgAge.count shouldBe 1
    avgAge.first.getDouble(0) shouldBe 22.5

    val maxAge = dataframe.agg(Map("age" -> "max"))
    maxAge.count shouldBe 1
    maxAge.first.getLong(0) shouldBe 24

    val sumAge = dataframe.agg(Map("age" -> "sum"))
    sumAge.count shouldBe 1
    sumAge.first.getLong(0) shouldBe 90
  }

  test("sql") {
    dataset.createOrReplaceTempView("persons")
    val persons = dataset.sqlContext.sql("select * from persons where age >= 21 and age <= 24 order by age").as[Person].cache
    persons.count shouldBe 4
    persons.head.name shouldBe "betty"
    persons.head.age shouldBe 21
  }
}