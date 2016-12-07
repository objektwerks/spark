package spark

import org.scalatest.FunSuite

class DatasetTest extends FunSuite {
  val session = SparkInstance.sparkSession
  val context = SparkInstance.sparkSession.sparkContext

  test("json > case class > dataset") {
    import session.implicits._
    val ds = session.read.json(context.makeRDD(SparkInstance.personJson)).as[Person]
    assert(ds.count == 4)
    assert(ds.filter(_.age == 24).first.name == "fred")
  }
}