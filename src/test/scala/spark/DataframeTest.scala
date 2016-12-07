package spark

import org.scalatest.FunSuite

class DataframeTest extends FunSuite {
  val session = SparkInstance.sparkSession
  val context = SparkInstance.sparkSession.sparkContext

  test("dataframe") {
    val df = session.read.json(context.makeRDD(SparkInstance.personJson))

    val names = df.select("name").orderBy("name").collect
    assert(names.length == 4)
    assert(names.head.mkString == "barney")

    val ages = df.select("age").orderBy("age").collect
    assert(ages.length == 4)
    assert(ages.head.getLong(0) == 21)

    var row = df.filter(df("age") > 23).first
    assert(row.getLong(0) == 24)
    assert(row.getAs[String](1) == "fred")

    row = df.agg(Map("age" -> "max")).first
    assert(row.getLong(0) == 24)

    row = df.agg(Map("age" -> "avg")).first
    assert(row.getDouble(0) == 22.5)
  }
}