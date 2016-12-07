package spark

import org.scalatest.FunSuite

class DatasetTest extends FunSuite {
  val session = SparkInstance.sparkSession
  val context = SparkInstance.sparkSession.sparkContext

  test("json > case class > dataset") {
    import session.implicits._
    val dataset = session.read.json(context.makeRDD(SparkInstance.personJson)).as[Person]
    assert(dataset.count == 4)
    assert(dataset.filter(_.age == 24).first.name == "fred")
  }
}