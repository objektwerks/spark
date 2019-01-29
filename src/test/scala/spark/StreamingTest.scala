package spark

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
    println(s"Batch Word Count: ${buffer.size}")
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
    println(s"Window Word Count: ${buffer.size}")
    buffer.sortBy(_._1).foreach(println)
    buffer.size shouldBe 96
  }
}