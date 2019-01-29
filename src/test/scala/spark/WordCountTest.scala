package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

case class Count(value: String, count: Long)
object Count {
  implicit val countSchema = Encoders.product[Count].schema
}

class WordCountTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("dataset") {
    val lines: Dataset[String] = sparkSession.read.textFile("./data/words/getttyburg.address.txt")
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
    val lines: Dataset[Row] = sparkSession.read.textFile("./data/words/getttyburg.address.txt").toDF("line")
    val counts = lines
      .flatMap(row => row.getString(0).split("\\W+"))
      .filter(_.nonEmpty)
      .groupByKey(_.toLowerCase)
      .count
      .collect
    counts.length shouldBe 138
  }

  test("rdd") {
    val lines = sparkContext.textFile("./data/words/getttyburg.address.txt")
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
      .option("basePath", "./data/words")
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
      .awaitTermination(10000L)
    val words = sqlContext.sql("select * from words").cache
    words.count shouldBe 138
    words.sort("value").show(numRows = 138, truncate = false)
  }

  test("dstream") {
    val streamingContext = new StreamingContext(sparkContext, batchDuration = Milliseconds(100))
    val dstream = textToDStream("./data/words/getttyburg.address.txt", streamingContext)
    val wordCountDstream = countWords(dstream)
    val buffer = mutable.ArrayBuffer[(String, Int)]()
    wordCountDstream foreachRDD { rdd => buffer ++= rdd.collect }
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    buffer.size shouldBe 138
  }

  private def textToDStream(filePath: String, streamingContext: StreamingContext): DStream[String] = {
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    val lines = sparkContext.textFile(filePath)
    queue += lines
    dstream
  }

  private def countWords(ds: DStream[String]): DStream[(String, Int)] = {
    ds.flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }
}