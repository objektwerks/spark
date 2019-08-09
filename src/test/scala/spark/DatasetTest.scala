package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{FunSuite, Matchers}
import spark.entity.{Person, Task}

class DatasetTest extends FunSuite with Matchers {
  import SparkInstance._
  import org.apache.spark.sql.expressions._
  import org.apache.spark.sql.functions._
  import sparkSession.implicits._

  val dataset = sparkSession.read.json("./data/person/person.json").as[Person].cache

  test("dataset") {
    dataset.printSchema
    dataset.count shouldBe 4
    assert(dataset.toDF.isInstanceOf[Dataset[Row]])
    assert(dataset.rdd.isInstanceOf[RDD[Person]])
    dataset.describe("age").show
  }

  test("map") {
    val mapNameToUpperCase = dataset.map(_.name.toUpperCase).cache
    mapNameToUpperCase.count shouldBe 4
    mapNameToUpperCase.head shouldBe "FRED"
  }

  test("filter") {
    val filterByName = dataset.filter(_.name == "barney").cache
    filterByName.count shouldBe 1
    filterByName.head.name shouldBe "barney"

    val filterByAge = dataset.filter(_.age > 23).cache
    filterByAge.count shouldBe 1
    filterByAge.head.age shouldBe 24
  }

  test("map -> filter") {
    val betty = dataset.map(_.name.toUpperCase).filter(_ == "BETTY").cache
    betty.count shouldBe 1
    betty.head shouldBe "BETTY"
  }

  test("select > where") {
    val selectNameByAge = dataset.select('name).where("age == 24").as[String].cache
    selectNameByAge.count shouldBe 1
    selectNameByAge.head shouldBe "fred"
  }

  test("sort > orderBy") {
    val sortByName = dataset.sort("name").cache
    sortByName.count shouldBe 4
    sortByName.head.name shouldBe "barney"

    val orderByName = dataset.select("name").orderBy("name").as[String].cache
    orderByName.count shouldBe 4
    orderByName.head shouldBe "barney"
  }

  test("agg") {
    dataset.select(min(col("age"))).head.getLong(0) shouldBe 21
    dataset.select(max(col("age"))).head.getLong(0) shouldBe 24
    dataset.select(avg(col("age"))).head.getDouble(0) shouldBe 22.5
    dataset.select(sum(col("age"))).head.getLong(0) shouldBe 90

    dataset.agg("age" -> "min").head.getLong(0) shouldBe 21
    dataset.agg("age" -> "avg").head.getDouble(0) shouldBe 22.5
    dataset.agg("age" -> "max").head.getLong(0) shouldBe 24
    dataset.agg("age" -> "sum").head.getLong(0) shouldBe 90
  }

  test("groupBy -> agg") {
    val groupByRole = dataset.groupBy("role").avg("age").as[(String, Double)].cache
    groupByRole.count shouldBe 2
    groupByRole.collect.map {
      case ("husband", avgAge) => avgAge shouldBe 23.0
      case ("wife", avgAge) => avgAge shouldBe 22.0
      case (_, _) => throw new IllegalArgumentException("GroupByRole test failed!")
    }
    groupByRole.show
  }

  test("window") {
    val window = Window.partitionBy("role").orderBy($"age".desc)
    val ranking = rank.over(window).as("rank")
    val result = dataset.select(col("role"), col("name"), col("age"), ranking).as[(String, String, Long, Int)].cache
    ("wife", "wilma", 23, 1) shouldEqual result.head
    result.show
  }

  test("join") {
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