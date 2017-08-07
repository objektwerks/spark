package spark

import org.scalatest.FunSuite

class DataframeTest extends FunSuite {
  test("dataframe") {
    import SparkInstance._
    import sparkSession.implicits._

    val dataframe = sparkSession.read.json(personJson.toDS()).as[Person]

    val names = dataframe.select("name").orderBy("name").collect
    assert(names.length == 4)
    assert(names.head.mkString == "barney")

    val ages = dataframe.select("age").orderBy("age").collect
    assert(ages.length == 4)
    assert(ages.head.getLong(0) == 21)

    val fred = dataframe.filter(dataframe("age") > 23).first
    assert(fred.age == 24)
    assert(fred.name == "fred")

    val minAge = dataframe.agg(Map("age" -> "min")).first
    assert(minAge.getLong(0) == 21)

    val avgAge = dataframe.agg(Map("age" -> "avg")).first
    assert(avgAge.getDouble(0) == 22.5)
  }
}