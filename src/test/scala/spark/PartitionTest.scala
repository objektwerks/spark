package spark

import java.util.UUID

import org.scalatest.{FunSuite, Matchers}

class PartitionTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  val dataframe = (1 to 10).toList.toDF("number")

  test("partition") {
    dataframe.rdd.partitions.length shouldEqual 8
    dataframe.write.csv(s"./target/partitioned-numbers-${UUID.randomUUID.toString}")
  }

  test("coalesce") {
    val coalesced = dataframe.coalesce(2)
    coalesced.rdd.partitions.length shouldEqual 2
    coalesced.write.csv(s"./target/coalesced-numbers-${UUID.randomUUID.toString}")
  }

  test("repartition") {
    dataframe.repartition(4).rdd.partitions.length shouldEqual 4
    dataframe.repartition(2).rdd.partitions.length shouldEqual 2
  }

  test("partitionBy") {
    val dataframe = sparkSession.read.json("./data/person/person.json").cache
    val outputPath = new java.io.File(s"./target/partitionby-roles-${UUID.randomUUID.toString}").getCanonicalPath
    dataframe
      .repartition(2)
      .write
      .partitionBy("role")
      .parquet(outputPath)
  }
}