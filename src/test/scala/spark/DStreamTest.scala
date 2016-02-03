package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.FunSuite

import scala.collection.mutable

class DStreamTest extends FunSuite {
  val conf = SparkInstance.conf
  val context = SparkInstance.context

  test("dstream") {
    val streamingContext = new StreamingContext(context, Milliseconds(1000))
    val queue = mutable.Queue[RDD[String]]()
    val ds = streamingContext.queueStream(queue)
    queue += context.makeRDD(SparkInstance.license)
    val wordCountDs = countWords(ds)
    wordCountDs.saveAsTextFiles("./target/output/test/ds")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("window") {
    val streamingContext = new StreamingContext(context, Milliseconds(1000))
    val queue = mutable.Queue[RDD[String]]()
    val ds = streamingContext.queueStream(queue)
    queue += context.makeRDD(SparkInstance.license)
    val wordCountDs = countWords(ds, windowLengthInMillis = 2000, slideIntervalInMillis = 1000)
    wordCountDs.saveAsTextFiles("./target/output/test/ds/window")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(1000)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }
}