package spark

import org.scalatest.{FunSuite, Matchers}

class DatasetTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("dataset") {
    val dataset = sparkSession.read.json("./data/json/person.json").as[Person].cache
    dataset.printSchema
    dataset.count shouldBe 4

    val filterByName = dataset.filter(_.name == "barney").cache
    filterByName.count shouldBe 1
    filterByName.head.name shouldBe "barney"

    val filterByAge = dataset.filter(_.age > 23).cache
    filterByAge.count shouldBe 1
    filterByAge.head.age shouldBe 24

    val selectNameByAge = dataset.select("name").where("age == 24").as[String].cache
    selectNameByAge.count shouldBe 1
    selectNameByAge.head shouldBe "fred"

    val orderByName = dataset.select("name").orderBy("name").as[String].cache
    orderByName.count shouldBe 4
    orderByName.head shouldBe "barney"

    import Person._
    dataset.map(_.age).collect.min shouldBe 21
    dataset.map(_.age).collect.avg shouldBe 22.5
    dataset.map(_.age).collect.max shouldBe 24
    dataset.map(_.age).collect.sum shouldBe 90
    dataset.map(_.age).reduce(_ + _) shouldBe 90

    dataset.agg(Map("age" -> "min")).first.getLong(0) shouldBe 21
    dataset.agg(Map("age" -> "avg")).first.getDouble(0) shouldBe 22.5
    dataset.agg(Map("age" -> "max")).first.getLong(0) shouldBe 24
    dataset.agg(Map("age" -> "sum")).first.getLong(0) shouldBe 90

    import org.apache.spark.sql.functions._
    dataset.agg(min(dataset("age"))).head.getLong(0) shouldBe 21
    dataset.agg(avg(dataset("age"))).head.getDouble(0) shouldBe 22.5
    dataset.agg(max(dataset("age"))).head.getLong(0) shouldBe 24
    dataset.agg(sum(dataset("age"))).head.getLong(0) shouldBe 90

    val groupByRole = dataset.groupBy("role").avg("age").as[(String, Double)].cache
    groupByRole.count shouldBe 2
    groupByRole.collect.map {
      case ("husband", avgAge) => avgAge shouldBe 23.0
      case ("wife", avgAge) => avgAge shouldBe 22.0
    }
    val groupByRoleMap = groupByRole.collect.map(roleAvgAge => roleAvgAge._1 -> roleAvgAge._2).toMap[String, Double]
    groupByRoleMap("husband") shouldBe 23.0
    groupByRoleMap("wife") shouldBe 22.0

    dataset.describe("age").collect.foreach(println)
  }
}