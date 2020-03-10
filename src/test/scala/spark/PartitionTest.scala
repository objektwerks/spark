package spark

import java.util.UUID

import org.scalatest.{FunSuite, Matchers}

class PartitionTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  val dataframe = (1 to 10).toList.toDF("number")

  test("partition") {
    dataframe.rdd.partitions.length shouldEqual 8
    dataframe.write.csv(s"./target/${UUID.randomUUID.toString}-partitioned-numbers")
  }

  test("coalesce") {
    val coalesced = dataframe.coalesce(2)
    coalesced.rdd.partitions.length shouldEqual 2
    coalesced.write.csv(s"./target/${UUID.randomUUID.toString}-coalesced-numbers")
  }

  test("repartition") {
    dataframe.repartition(4).rdd.partitions.length shouldEqual 4
    dataframe.repartition(2).rdd.partitions.length shouldEqual 2
  }
}