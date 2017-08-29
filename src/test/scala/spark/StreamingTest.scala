package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

class StreamingTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("dstream") {
    val streamingContext = new StreamingContext(sparkContext, Milliseconds(100))
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    queue += sparkContext.makeRDD(licenseText)
    val wordCountDstream = countWords(dstream)
    val count = mutable.ArrayBuffer[Int]()
    wordCountDstream foreachRDD { rdd => count += rdd.map(_._2).sum.toInt }
    wordCountDstream.saveAsTextFiles("./target/output/test/ds")
    streamingContext.checkpoint("./target/output/test/ds/checkpoint")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    count.sum shouldBe 169
  }

  test("window") {
    val streamingContext = new StreamingContext(sparkContext, Milliseconds(100))
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    queue += sparkContext.makeRDD(licenseText)
    val wordCountDstream = countWords(dstream, windowLengthInMillis = 100, slideIntervalInMillis = 100)
    val count = mutable.ArrayBuffer[Int]()
    wordCountDstream foreachRDD { rdd => count += rdd.map(_._2).sum.toInt }
    wordCountDstream.saveAsTextFiles("./target/output/test/window")
    streamingContext.checkpoint("./target/output/test/window/checkpoint")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    count.sum shouldBe 169
  }

  test("structured") {
    import Person._
    val in = sparkSession.readStream
      .option("basePath", "./data/json")
      .schema(personStructType)
      .json("./data/json")
      .as[Person]
    val out = in.writeStream
      .option("checkpointLocation", "./target/output/test/ss/checkpoint")
      .foreach(personForeachWriter)
    val query = out.start()
    query.awaitTermination(1000L)
  }
}