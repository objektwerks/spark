package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.FunSuite

import scala.collection.mutable

class DStreamTest extends FunSuite {
  import SparkInstance._

  val context = sparkSession.sparkContext

  test("dstream") {
    val streamingContext = new StreamingContext(context, Milliseconds(100))
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    queue += context.makeRDD(licenseText)
    val wordCountDstream = countWords(dstream)
    val count = mutable.ArrayBuffer[Int]()
    wordCountDstream foreachRDD { rdd => count += rdd.map(_._2).sum.toInt }
    wordCountDstream.saveAsTextFiles("./target/output/test/ds")
    streamingContext.checkpoint("./target/output/test/ds/checkpoint")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    assert(count.sum == 169)
  }

  test("window") {
    val streamingContext = new StreamingContext(context, Milliseconds(100))
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    queue += context.makeRDD(licenseText)
    val wordCountDstream = countWords(dstream, windowLengthInMillis = 100, slideIntervalInMillis = 100)
    val count = mutable.ArrayBuffer[Int]()
    wordCountDstream foreachRDD { rdd => count += rdd.map(_._2).sum.toInt }
    wordCountDstream.saveAsTextFiles("./target/output/test/window")
    streamingContext.checkpoint("./target/output/test/window/checkpoint")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    assert(count.sum == 169)
  }
}