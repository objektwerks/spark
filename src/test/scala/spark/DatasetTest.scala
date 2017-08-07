package spark

import org.scalatest.FunSuite

class DatasetTest extends FunSuite {
  test("dataset") {
    import SparkInstance._
    import sparkSession.implicits._

    val dataset = sparkSession.read.json(personJson.toDS()).as[Person]

    assert(dataset.count == 4)
    assert(dataset.filter(_.age == 24).first.name == "fred")

    val names = dataset.select("name").orderBy("name").collect
    assert(names.length == 4)
    assert(names.head.mkString == "barney")

    val ages = dataset.select("age").orderBy("age").collect
    assert(ages.length == 4)
    assert(ages.head.getLong(0) == 21)
  }
}