package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.FunSuite

import scala.collection.mutable

class DStreamTest extends FunSuite {
  val conf = SparkInstance.conf
  val context = SparkInstance.context

  test("dstream") {
    val streamingContext = new StreamingContext(context, Milliseconds(100))
    val queue = mutable.Queue[RDD[String]]()
    val ds = streamingContext.queueStream(queue)
    queue += context.makeRDD(SparkInstance.license)
    val wordCountDs = countWords(ds)
    val count = mutable.ArrayBuffer[Int]()
    wordCountDs foreachRDD { rdd => count += rdd.map(_._2).sum.toInt }
    wordCountDs.saveAsTextFiles("./target/output/test/ds")
    streamingContext.checkpoint("./target/output/test/ds/checkpoint")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    assert(count.sum == 169)
  }

  test("window") {
    val streamingContext = new StreamingContext(context, Milliseconds(100))
    val queue = mutable.Queue[RDD[String]]()
    val ds = streamingContext.queueStream(queue)
    queue += context.makeRDD(SparkInstance.license)
    val wordCountDs = countWords(ds, windowLengthInMillis = 100, slideIntervalInMillis = 100)
    val count = mutable.ArrayBuffer[Int]()
    wordCountDs foreachRDD { rdd => count += rdd.map(_._2).sum.toInt }
    wordCountDs.saveAsTextFiles("./target/output/test/window")
    streamingContext.checkpoint("./target/output/test/window/checkpoint")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    assert(count.sum == 169)
  }
}