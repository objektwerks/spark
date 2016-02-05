package spark

import org.scalatest.FunSuite

class DataframeTest extends FunSuite {
  val context = SparkInstance.context
  val sqlContext = SparkInstance.sqlContext

  test("dataframe") {
    val df = sqlContext.read.json(context.makeRDD(SparkInstance.personJson))

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

  test("json > case class > dataframe") {
    val rdd = sqlContext.read.json(context.makeRDD(SparkInstance.personJson)).map(p => Person(p(0).asInstanceOf[Long], p(1).asInstanceOf[String]))
    val df = sqlContext.createDataFrame[Person](rdd)
    df.registerTempTable("persons")

    val names = df.select("name").orderBy("name").collect
    assert(names.length == 4)
    assert(names.head.mkString == "barney")

    val ages = df.select("age").orderBy("age").collect
    assert(ages.length == 4)
    assert(ages.head.getLong(0) == 21)
  }
}