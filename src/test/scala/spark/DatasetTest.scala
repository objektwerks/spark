package spark

import org.scalatest.FunSuite

class DatasetTest extends FunSuite {
  val session = SparkInstance.sparkSession
  val context = SparkInstance.sparkSession.sparkContext

  test("dataset") {
    import session.implicits._
    val dataset = session.read.json(context.makeRDD(SparkInstance.personJson)).as[Person]

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