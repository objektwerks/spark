package spark

import org.scalatest.{FunSuite, Matchers}

class DatasetTest extends FunSuite with Matchers {
  import SparkInstance._
  import org.apache.spark.sql.functions._
  import sparkSession.implicits._

  test("dataset") {
    val dataset = sparkSession.read.json("./data/person/person.json").as[Person].cache
    dataset.printSchema
    dataset.count shouldBe 4

    val sortByName = dataset.sort("name").cache
    sortByName.count shouldBe 4
    sortByName.head.name shouldBe "barney"

    val selectNameByAge = dataset.select("name").where("age == 24").as[String].cache
    selectNameByAge.count shouldBe 1
    selectNameByAge.head shouldBe "fred"

    val orderByName = dataset.select("name").orderBy("name").as[String].cache
    orderByName.count shouldBe 4
    orderByName.head shouldBe "barney"

    val filterByName = dataset.filter(_.name == "barney").cache
    filterByName.count shouldBe 1
    filterByName.head.name shouldBe "barney"

    val filterByAge = dataset.filter(_.age > 23).cache
    filterByAge.count shouldBe 1
    filterByAge.head.age shouldBe 24

    dataset.select(min(col("age"))).head.getLong(0) shouldBe 21
    dataset.select(max(col("age"))).head.getLong(0) shouldBe 24
    dataset.select(avg(col("age"))).head.getDouble(0) shouldBe 22.5
    dataset.select(sum(col("age"))).head.getLong(0) shouldBe 90

    dataset.agg("age" -> "min").head.getLong(0) shouldBe 21
    dataset.agg("age" -> "avg").head.getDouble(0) shouldBe 22.5
    dataset.agg("age" -> "max").head.getLong(0) shouldBe 24
    dataset.agg("age" -> "sum").head.getLong(0) shouldBe 90

    val groupByRole = dataset.groupBy("role").avg("age").as[(String, Double)].cache
    groupByRole.count shouldBe 2
    groupByRole.collect.map {
      case ("husband", avgAge) => avgAge shouldBe 23.0
      case ("wife", avgAge) => avgAge shouldBe 22.0
      case (_, _) => throw new IllegalArgumentException("GroupByRole test failed!")
    }
    groupByRole.show
  }

  test("dataset join") {
    val persons = sparkSession.read.json("./data/person/person.json").as[Person].cache
    val tasks = sparkSession.read.json("./data/task/task.json").as[Task].cache
    persons.count shouldBe 4
    tasks.count shouldBe 4

    val joinBy = persons.col("id") === tasks.col("pid")
    val personsTasks = persons.join(tasks, joinBy)
    personsTasks.count shouldBe 4
    personsTasks.show
  }
}