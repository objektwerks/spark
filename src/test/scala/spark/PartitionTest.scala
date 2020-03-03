package spark

import org.scalatest.{FunSuite, Matchers}

class PartitionTest  extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("partitions") {
    val dataframe = (1 to 10).toList.toDF
    dataframe.rdd.partitions.length shouldEqual 8
  }

}
