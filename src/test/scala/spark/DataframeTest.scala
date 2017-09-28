package spark

import org.apache.spark.sql.Row
import org.scalatest.{FunSuite, Matchers}

class DataframeTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("dataframe") {
    val dataframe = sparkSession.read.json("./data/json/person.json").cache
    dataframe.count shouldBe 4

    val selectByName = dataframe.select("name").where("name == 'barney'").cache
    selectByName.count shouldBe 1
    selectByName.first.getString(0) shouldBe "barney"

    val selectByAge = dataframe.select("age").where("age > 23").cache
    selectByAge.count shouldBe 1
    selectByAge.first.getLong(0) shouldBe 24

    val selectNameByAge = dataframe.select("name").where("age == 24").as[String].cache
    selectNameByAge.count shouldBe 1
    selectNameByAge.head shouldBe "fred"

    val orderByName = dataframe.select("name").orderBy("name").as[String].cache
    orderByName.count shouldBe 4
    orderByName.head shouldBe "barney"

    dataframe.agg(Map("age" -> "min")).first.getLong(0) shouldBe 21
    dataframe.agg(Map("age" -> "avg")).first.getDouble(0) shouldBe 22.5
    dataframe.agg(Map("age" -> "max")).first.getLong(0) shouldBe 24
    dataframe.agg(Map("age" -> "sum")).first.getLong(0) shouldBe 90

    import org.apache.spark.sql.functions._
    dataframe.agg(min(dataframe("age"))).head.getLong(0) shouldBe 21
    dataframe.agg(avg(dataframe("age"))).head.getDouble(0) shouldBe 22.5
    dataframe.agg(max(dataframe("age"))).head.getLong(0) shouldBe 24
    dataframe.agg(sum(dataframe("age"))).head.getLong(0) shouldBe 90

    val groupByRole = dataframe.groupBy("role").avg("age").cache
    groupByRole.count shouldBe 2
    groupByRole.collect.map {
      case Row("husband", avgAge) => avgAge shouldBe 23.0
      case Row("wife", avgAge) => avgAge shouldBe 22.0
    }
    val groupByRoleMap = groupByRole.collect.map(row => row.getString(0) -> row.getDouble(1)).toMap[String, Double]
    groupByRoleMap("husband") shouldBe 23.0
    groupByRoleMap("wife") shouldBe 22.0
  }
}