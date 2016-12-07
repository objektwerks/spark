package spark

import org.scalatest.FunSuite

class DataframeTest extends FunSuite {
  val session = SparkInstance.sparkSession
  val context = SparkInstance.sparkSession.sparkContext

  test("dataframe") {
    val dataframe = session.read.json(context.makeRDD(SparkInstance.personJson))

    val names = dataframe.select("name").orderBy("name").collect
    assert(names.length == 4)
    assert(names.head.mkString == "barney")

    val ages = dataframe.select("age").orderBy("age").collect
    assert(ages.length == 4)
    assert(ages.head.getLong(0) == 21)

    var row = dataframe.filter(dataframe("age") > 23).first
    assert(row.getLong(0) == 24)
    assert(row.getAs[String](1) == "fred")

    row = dataframe.agg(Map("age" -> "max")).first
    assert(row.getLong(0) == 24)

    row = dataframe.agg(Map("age" -> "avg")).first
    assert(row.getDouble(0) == 22.5)
  }
}