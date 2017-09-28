package spark

import org.scalatest.{FunSuite, Matchers}

class SqlTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("sql") {
    val dataset = sparkSession.read.json("./data/json/person.json").as[Person].cache
    dataset.count shouldBe 4
    dataset.createOrReplaceTempView("persons")
    val persons = dataset.sqlContext.sql("select * from persons where age >= 21 and age <= 22 order by age").as[Person].cache
    persons.count shouldBe 2
    persons.head.name shouldBe "betty"
    persons.head.age shouldBe 21
  }
}