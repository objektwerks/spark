package spark

import org.scalatest.{FunSuite, Matchers}

class WordCountTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

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
