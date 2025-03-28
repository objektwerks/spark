package objektwerks

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataframeTest extends AnyFunSuite with Matchers {
  import SparkInstance._
  import org.apache.spark.sql.expressions._
  import org.apache.spark.sql.functions._
  import sparkSession.implicits._

  val dataframe = sparkSession.read.json("./data/person/person.json").cache
  dataframe.write.json("./target/dataframe/person.json")

  test("dataframe") {
    dataframe.count shouldBe 4
    assert(dataframe.isInstanceOf[Dataset[Row]])
    assert(dataframe.as[Person].isInstanceOf[Dataset[Person]])
  }

  test("column") {
    val idColumn = dataframe.col("id")
    val nameColumn = col("name")
    val ageColumn = column("age")
    val roleColumn = expr("role")
    dataframe.select(idColumn, nameColumn, ageColumn, roleColumn).count shouldBe 4
  }

  test("selectExpr") {
    dataframe.selectExpr("id", "name", "age", "role").count shouldBe 4
  }

  test("extend") {
    dataframe.withColumn("dog_age", $"age" * 7).head.getLong(4) shouldBe 168
  }  

  test("update") {
    val incrementAgeNameToUpper = dataframe
      .withColumn("age", $"age" + 1)
      .withColumn("name", upper($"name"))
      .cache
    incrementAgeNameToUpper.count shouldBe 4
    incrementAgeNameToUpper.head.getLong(0) shouldBe 25
    incrementAgeNameToUpper.head.getString(2) shouldBe "FRED"
  }

  test("transform") {
    def incrementAge(df: DataFrame): DataFrame = df.withColumn("age", $"age" + 1)
    def nameToUpper(df: DataFrame): DataFrame = df.withColumn("name", upper($"name"))
    val incrementAgeNameToUpper = dataframe
      .transform(incrementAge)
      .transform(nameToUpper)
      .cache
    incrementAgeNameToUpper.count shouldBe 4
    incrementAgeNameToUpper.head.getLong(0) shouldBe 25
    incrementAgeNameToUpper.head.getString(2) shouldBe "FRED"
  }

  test("filter") {
    val filterByName = dataframe.filter("name == 'barney'").cache
    filterByName.count shouldBe 1
    filterByName.head.getLong(0) shouldBe 22
    filterByName.head.getString(2) shouldBe "barney"
    filterByName.head.getString(3) shouldBe "husband"
  }

  test("select > where") {
    val selectByName = dataframe.select("name").where("name == 'barney'").cache
    selectByName.count shouldBe 1
    selectByName.head.getString(0) shouldBe "barney"

    val selectByAge = dataframe.select("age").where("age > 23").cache
    selectByAge.count shouldBe 1
    selectByAge.head.getLong(0) shouldBe 24
  }

  test("select > orderBy") {
    val orderByName = dataframe.select("name").orderBy("name").cache
    orderByName.count shouldBe 4
    orderByName.head.getString(0) shouldBe "barney"
  }

  test("sort") {
    val sortByName = dataframe.sort("name").cache
    sortByName.count shouldBe 4
    sortByName.head.getLong(0) shouldBe 22
    sortByName.head.getString(2) shouldBe "barney"
    sortByName.head.getString(3) shouldBe "husband"
  }

  test("agg") {
    dataframe.agg("age" -> "min").head.getLong(0) shouldBe 21
    dataframe.agg("age" -> "avg").head.getDouble(0) shouldBe 22.5
    dataframe.agg("age" -> "max").head.getLong(0) shouldBe 24
    dataframe.agg("age" -> "sum").head.getLong(0) shouldBe 90
  }

  test("select > agg") {
    dataframe.select(min(col("age"))).head.getLong(0) shouldBe 21
    dataframe.select(max(col("age"))).head.getLong(0) shouldBe 24
    dataframe.select(avg(col("age"))).head.getDouble(0) shouldBe 22.5
    dataframe.select(sum(col("age"))).head.getLong(0) shouldBe 90
  }

  test("select > agg > case class") {
    dataframe.select(min(col("age"))).map(row => Age(row.getLong(0))).head shouldBe Age(21)
    dataframe.select(max(col("age"))).map(row => Age(row.getLong(0))).head shouldBe Age(24)
  }

  test("groupBy > avg") {
    val groupByRole = dataframe
      .groupBy("role")
      .avg("age")
      .cache
    groupByRole.count shouldBe 2
    groupByRole.collect.foreach {
      case Row("husband", avgAge) => avgAge shouldBe 23.0
      case Row("wife", avgAge) => avgAge shouldBe 22.0
    }
  }

  test("groupBy > agg(min, avg, max)") {
    val groupByRole = dataframe
      .groupBy("role")
      .agg(
        min("age"),
        avg("age"),
        max("age")
      )
      .cache
    groupByRole.count shouldBe 2
    groupByRole.collect.foreach {
      case Row("husband", minAge, avgAge, maxAge) =>
        minAge shouldBe 22
        avgAge shouldBe 23.0
        maxAge shouldBe 24
      case Row("wife", minAge, avgAge, maxAge) => 
        minAge shouldBe 21
        avgAge shouldBe 22.0
        maxAge shouldBe 23
    }
  }

  test("when > otherwise") {
    val personsWithGender = dataframe.withColumn("gender", when($"role" === "husband", "male").otherwise("female"))
    personsWithGender.collect.foreach {
      case Row(_, _, _, "husband", gender ) => gender shouldBe "male"
      case Row(_, _, _, "wife", gender) => gender shouldBe "female"
    }
  }

  test("window") {
    val window = Window.partitionBy("role").orderBy($"age".desc)
    val ranking = rank.over(window).as("rank")
    val result = dataframe.select(col("role"), col("name"), col("age"), ranking).as[(String, String, Long, Int)].cache
    ("wife", "wilma", 23, 1) shouldEqual result.head
  }

  test("join") {
    val persons = sparkSession.read.json("./data/person/person.json").cache
    val tasks = sparkSession.read.json("./data/task/task.json").cache
    persons.count shouldBe 4
    tasks.count shouldBe 4

    val joinBy = persons.col("id") === tasks.col("pid")
    val personsTasks = persons.join(tasks, joinBy)
    personsTasks.count shouldBe 4
  }
}