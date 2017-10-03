package spark

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{FunSuite, Matchers}

class SqlTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("dataframe sql") {
    val dataframe = sparkSession.read.json("./data/json/person.json").cache
    assert(dataframe.isInstanceOf[Dataset[Row]])
    dataframe.count shouldBe 4

    dataframe.createOrReplaceTempView("persons")

    val rows = sqlContext.sql("select * from persons where age >= 21 and age <= 22 order by age").cache
    rows.count shouldBe 2
    rows.head.getString(1) shouldBe "betty"
    rows.head.getLong(0) shouldBe 21

    sqlContext.sql("select min(age) from persons").take(1)(0).getLong(0) shouldBe 21
    sqlContext.sql("select max(age) from persons").take(1)(0).getLong(0) shouldBe 24
  }

  test("dataset sql") {
    val dataset = sparkSession.read.json("./data/json/person.json").as[Person].cache
    dataset.count shouldBe 4

    dataset.createOrReplaceTempView("persons")

    val persons = sqlContext.sql("select * from persons where age >= 21 and age <= 22 order by age").as[Person].cache
    persons.count shouldBe 2
    persons.head.name shouldBe "betty"
    persons.head.age shouldBe 21

    sqlContext.sql("select min(age) from persons").as[Long].take(1)(0) shouldBe 21
    sqlContext.sql("select max(age) from persons").as[Long].take(1)(0) shouldBe 24
  }
}