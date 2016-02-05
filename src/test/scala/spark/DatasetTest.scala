package spark

import org.apache.spark.sql.expressions.Aggregator
import org.scalatest.FunSuite

class DatasetTest extends FunSuite {
  val context = SparkInstance.context
  val sqlContext = SparkInstance.sqlContext

  test("aggregator > dataset") {
    import sqlContext.implicits._
    val sum = new Aggregator[Int, Int, Int] with Serializable {
      def zero: Int = 0
      def reduce(y: Int, x: Int) = y + x
      def merge(y1: Int, y2: Int) = y1 + y2
      def finish(y: Int) = y
    }.toColumn

    val ds = Seq(1, 2, 3).toDS()
    assert(ds.select(sum).collect.apply(0) == 6)
  }

  test("json > case class > dataset") {
    import sqlContext.implicits._
    val ds = sqlContext.read.json(context.makeRDD(SparkInstance.personJson)).as[Person]
    assert(ds.count == 4)
    assert(ds.filter(_.age == 24).first.name == "fred")
  }

  test("json > case class > aggregator > dataset") {
    import sqlContext.implicits._
    val sum =  new Aggregator[Data, Long, Long] {
      def zero: Long = 0
      def reduce(y: Long, x: Data): Long = y + x.n
      def merge(y1: Long, y2: Long): Long = y1 + y2
      def finish(y: Long): Long = y
    }.toColumn

    val ds = sqlContext.read.json(context.makeRDD(SparkInstance.dataJson)).as[Data]
    assert(ds.select(sum).collect.apply(0) == 6)
  }
}