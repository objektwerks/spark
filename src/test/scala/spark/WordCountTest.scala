package spark

import org.apache.spark.sql.Dataset
import org.scalatest.{FunSuite, Matchers}

class WordCountTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("rdd") {
    val lines = sparkContext.textFile("./data/words/getttyburg.address.txt").cache
    lines.count shouldBe 5
    println(s"line count: ${lines.count}")

    val words = lines.flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect
      .toMap

    words.keys.size shouldBe 138
    println(s"unique word count: ${words.keys.size} ")
    for( (word, count) <- words) println(s"work: $word, count: $count")
  }

  test("dataset") {
    val lines: Dataset[String] = sparkSession.read.textFile("./data/words/getttyburg.address.txt").cache
    lines.count shouldBe 5
    println(s"line count: ${lines.count}")

    val words = lines
      .flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .groupBy("value")
      .count
      .collect

    words.length shouldBe 138
    println(s"unique word count: ${words.length} ")
    words foreach println
  }

  test("structured streaming") {
    val lines = sparkSession
      .readStream
      .option("basePath", "./data/words")
      .text("./data/words")
    val words = lines
      .as[String]
      .flatMap(_.split("\\W+"))
      .groupBy("value")
      .count
    val query = words
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination(6000L)
  }
}
