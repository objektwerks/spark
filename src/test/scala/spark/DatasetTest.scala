package spark

import org.apache.spark.sql.expressions.Aggregator
import org.scalatest.FunSuite

class DatasetTest extends FunSuite {
  val conf = SparkInstance.conf
  val context = SparkInstance.context
  val sqlContext = SparkInstance.sqlContext

  test("aggregator > dataset") {
    import sqlContext.implicits._
    val sum = new Aggregator[Int, Int, Int] with Serializable {
      def zero: Int = 0
      def reduce(b: Int, a: Int) = b + a
      def merge(b1: Int, b2: Int) = b1 + b2
      def finish(b: Int) = b
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
      def reduce(b: Long, a: Data): Long = b + a.n
      def merge(b1: Long, b2: Long): Long = b1 + b2
      def finish(b: Long): Long = b
    }.toColumn

    val ds = sqlContext.read.json(context.makeRDD(SparkInstance.dataJson)).as[Data]
    assert(ds.select(sum).collect.apply(0) == 6)
  }
}