package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

class StreamingTest extends FunSuite with Matchers {
  import SparkInstance._

  test("batch") {
    val streamingContext = new StreamingContext(sparkContext, batchDuration = Milliseconds(100))
    val dstream = textToDStream("./data/txt/license.txt", streamingContext)
    val wordCountDstream = countWords(dstream)
    val buffer = mutable.ArrayBuffer[(String, Int)]()
    wordCountDstream foreachRDD { rdd => buffer ++= rdd.collect }
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    println("Batch Word Count:")
    buffer.sortBy(_._1).foreach(println)
    buffer.size shouldBe 96
  }

  test("window") {
    val streamingContext = new StreamingContext(sparkContext, batchDuration = Milliseconds(200))
    val dstream = textToDStream("./data/txt/license.txt", streamingContext)
    val wordCountDstream = countWords(dstream, windowLengthInMillis = 200, slideIntervalInMillis = 200)
    val buffer = mutable.ArrayBuffer[(String, Int)]()
    wordCountDstream foreachRDD { rdd => buffer ++= rdd.collect }
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    println("Window Word Count:")
    buffer.sortBy(_._1).foreach(println)
    buffer.size shouldBe 96
  }

  def textToDStream(filePath: String, streamingContext: StreamingContext): DStream[String] = {
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    val lines = sparkContext.textFile(filePath)
    queue += lines
    dstream
  }

  def countWords(ds: DStream[String]): DStream[(String, Int)] = {
    ds.flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }

  def countWords(ds: DStream[String], windowLengthInMillis: Int, slideIntervalInMillis: Int): DStream[(String, Int)] = {
    ds.flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKeyAndWindow((x:Int, y:Int) => x + y, Milliseconds(windowLengthInMillis), Milliseconds(slideIntervalInMillis))
  }
}