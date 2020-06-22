package objektwerks

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class WordCountTest extends AnyFunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("dataset") {
    val lines: Dataset[String] = sparkSession.read.textFile("./data/words/gettysburg.address.txt")
    val counts = lines
      .flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .groupByKey(_.toLowerCase)
      .count
      .collect
      .map { case (line, count) => Count(line, count) }
    counts.length shouldBe 138
  }

  test("dataframe") {
    val lines: Dataset[Row] = sparkSession.read.textFile("./data/words/gettysburg.address.txt").toDF("line")
    val counts = lines
      .flatMap(row => row.getString(0).split("\\W+"))
      .filter(_.nonEmpty)
      .groupByKey(_.toLowerCase)
      .count
      .collect
    counts.length shouldBe 138
  }

  test("rdd") {
    val lines = sparkContext.textFile("./data/words/gettysburg.address.txt")
    val counts = lines.flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect
    counts.length shouldBe 138
  }

  test("structured streaming") {
    sparkSession
      .readStream
      .text("./data/words")
      .flatMap(row => row.getString(0).split("\\W+"))
      .filter(_.nonEmpty)
      .groupByKey(_.toLowerCase)
      .count
      .writeStream
      .queryName("words")
      .outputMode("complete")
      .format("memory")
      .start()
      .awaitTermination(3000L)
    val words = sparkSession.sql("select * from words")
    words.count shouldBe 138
  }

  test("dstream") {
    val streamingContext = new StreamingContext(sparkContext, batchDuration = Milliseconds(100))
    val dstream = textToDStream("./data/words/gettysburg.address.txt", streamingContext)
    val wordCountDstream = countWords(dstream)
    val buffer = mutable.ArrayBuffer[(String, Int)]()
    wordCountDstream foreachRDD { rdd => buffer ++= rdd.collect }
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    buffer.size shouldBe 138
  }
}