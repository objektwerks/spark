package spark

import org.scalatest.{FunSuite, Matchers}

class WordCountTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("rdd") {
    val lines = sparkContext.textFile("./data/words/getttyburg.address.txt").cache
    println(s"line count: ${lines.count}")

    val count = lines.flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect
      .toMap

    println(s"unique word count: ${count.keys.size} ")
    for( (word, count) <- count) println(s"work: $word, count: $count")
  }

  test("structured streaming") {
    val lines = sparkSession
      .readStream
      .option("basePath", "./data/words")
      .text("./data/words")
    val words = lines
      .as[String]
      .flatMap(_.split("\\W+"))
    val count = words
      .groupBy("value")
      .count
    val query = count
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination(6000L)
  }
}
