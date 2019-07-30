package spark

import org.apache.spark.sql.Row
import org.scalatest.{FunSuite, Matchers}

class DataframeTest extends FunSuite with Matchers {
  import SparkInstance._
  import org.apache.spark.sql.functions._

  test("dataframe") {
    val dataframe = sparkSession.read.json("./data/person/person.json").cache
    dataframe.printSchema
    dataframe.count shouldBe 4

    val sortByName = dataframe.sort("name").cache
    sortByName.count shouldBe 4
    sortByName.head.getLong(0) shouldBe 22
    sortByName.head.getString(2) shouldBe "barney"
    sortByName.head.getString(3) shouldBe "husband"

    val selectByName = dataframe.select("name").where("name == 'barney'").cache
    selectByName.count shouldBe 1
    selectByName.head.getString(0) shouldBe "barney"

    val selectByAge = dataframe.select("age").where("age > 23").cache
    selectByAge.count shouldBe 1
    selectByAge.head.getLong(0) shouldBe 24

    val orderByName = dataframe.select("name").orderBy("name").cache
    orderByName.count shouldBe 4
    orderByName.head.getString(0) shouldBe "barney"

    val filterByName = dataframe.filter("name == 'barney'").cache
    filterByName.count shouldBe 1
    filterByName.head.getLong(0) shouldBe 22
    filterByName.head.getString(2) shouldBe "barney"
    filterByName.head.getString(3) shouldBe "husband"

    dataframe.select(min(col("age"))).head.getLong(0) shouldBe 21
    dataframe.select(max(col("age"))).head.getLong(0) shouldBe 24
    dataframe.select(avg(col("age"))).head.getDouble(0) shouldBe 22.5
    dataframe.select(sum(col("age"))).head.getLong(0) shouldBe 90

    dataframe.agg("age" -> "min").head.getLong(0) shouldBe 21
    dataframe.agg("age" -> "avg").head.getDouble(0) shouldBe 22.5
    dataframe.agg("age" -> "max").head.getLong(0) shouldBe 24
    dataframe.agg("age" -> "sum").head.getLong(0) shouldBe 90

    val groupByRole = dataframe.groupBy("role").avg("age").cache
    groupByRole.count shouldBe 2
    groupByRole.collect.map {
      case Row("husband", avgAge) => avgAge shouldBe 23.0
      case Row("wife", avgAge) => avgAge shouldBe 22.0
    }
    groupByRole.show
  }
}