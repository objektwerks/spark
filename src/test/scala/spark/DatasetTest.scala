package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{FunSuite, Matchers}
import spark.entity.{Age, AvgAgeByRole, Person, Task}

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

  test("update") {
    val incrementAgeNameToUpper = dataset
      .withColumn("age", 'age + 1)
      .withColumn("name", upper('name))
      .as[Person]
      .cache
    incrementAgeNameToUpper.count shouldBe 4
    incrementAgeNameToUpper.head.age shouldBe 25
    incrementAgeNameToUpper.head.name shouldBe "FRED"
  }

  test("transform") {
    def incrementAge(ds: Dataset[Person]): Dataset[Person] = ds.withColumn("age", $"age" + 1).as[Person]
    def nameToUpper(ds: Dataset[Person]): Dataset[Person] = ds.withColumn("name", upper($"name")).as[Person]
    val incrementAgeNameToUpper = dataset
      .transform(incrementAge)
      .transform(nameToUpper)
      .cache
    incrementAgeNameToUpper.count shouldBe 4
    incrementAgeNameToUpper.head.age shouldBe 25
    incrementAgeNameToUpper.head.name shouldBe "FRED"
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

  test("filter -> map") {
    val betty = dataset.filter(_.name == "betty").map(_.name.toUpperCase).cache
    betty.count shouldBe 1
    betty.head shouldBe "BETTY"
  }

  test("sort > orderBy") {
    val sortByName = dataset.sort('name).cache
    sortByName.count shouldBe 4
    sortByName.head.name shouldBe "barney"

    val orderByName = dataset.select('name).orderBy('name).as[String].cache
    orderByName.count shouldBe 4
    orderByName.head shouldBe "barney"
  }

  test("agg > case class") {
    dataset.select(min(col("age"))).map(row => Age(row.getLong(0))).head shouldBe Age(21)
    dataset.select(max(col("age"))).map(row => Age(row.getLong(0))).head shouldBe Age(24)
  }

  test("groupByKey > agg") {
    dataset
      .groupByKey( _.role )
      .agg( typed.avg(_.age.toDouble) )
      .map( tuple => AvgAgeByRole(tuple._1, tuple._2) )
      .collect.map {
        case AvgAgeByRole("husband", avgAge) => avgAge shouldBe 23.0
        case AvgAgeByRole("wife", avgAge) => avgAge shouldBe 22.0
        case AvgAgeByRole(_, _) => throw new IllegalArgumentException("GroupByRole test failed!")
      }
  }

  test("groupBy -> agg") {
    val groupByRole = dataset.groupBy('role).avg("age").as[(String, Double)].cache
    groupByRole.count shouldBe 2
    groupByRole.collect.map {
      case ("husband", avgAge) => avgAge shouldBe 23.0
      case ("wife", avgAge) => avgAge shouldBe 22.0
      case (_, _) => throw new IllegalArgumentException("GroupByRole test failed!")
    }
    groupByRole.show
  }

  test("window") {
    val window = Window.partitionBy('role).orderBy($"age".desc)
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