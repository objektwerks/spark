package spark

import org.scalatest.{FunSuite, Matchers}

class PartitionTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("partitions") {
    val dataframe = (1 to 10).toList.toDF("number")
    dataframe.rdd.partitions.length shouldEqual 8
    dataframe.write.csv("./target/partitioned-numbers")
  }

  test("coalesce") {
    val dataframe = (1 to 10).toList.toDF("number")
    dataframe.rdd.partitions.length shouldEqual 8
    val coalesced = dataframe.coalesce(2)
    coalesced.rdd.partitions.length shouldEqual 2
    coalesced.write.csv("./target/coalesced-numbers")
  }
}