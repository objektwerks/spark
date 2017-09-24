package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

class StreamingTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("batch") {
    val streamingContext = new StreamingContext(sparkContext, batchDuration = Milliseconds(100))
    val dstream = textToDStream(licenseText, streamingContext)
    val wordCountDstream = countWords(dstream)
    val map = mutable.Map[String, Int]()
    wordCountDstream foreachRDD { rdd => map ++= rdd.collect.toMap[String, Int] }
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    println("Batch Word Count:")
    map.toSeq.sortBy(_._1).foreach(println)
    map.size shouldBe 96
  }

  test("window") {
    val streamingContext = new StreamingContext(sparkContext, batchDuration = Milliseconds(200))
    val dstream = textToDStream(licenseText, streamingContext)
    val wordCountDstream = countWords(dstream, windowLengthInMillis = 200, slideIntervalInMillis = 200)
    val map = mutable.Map[String, Int]()
    wordCountDstream foreachRDD { rdd => map ++= rdd.collect.toMap[String, Int] }
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
    println("Window Word Count:")
    map.toSeq.sortBy(_._1).foreach(println)
    map.size shouldBe 96
  }

  test("structured") {
    import Person._
    val in = sparkSession
      .readStream
      .option("basePath", "./data/json")
      .schema(personStructType)
      .json("./data/json")
      .as[Person]
    val out = in
      .writeStream
      .foreach(personForeachWriter)
    val query = out.start()
    query.awaitTermination(1000L)
  }

  def textToDStream(text: Seq[String], streamingContext: StreamingContext): InputDStream[String] = {
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    queue += sparkContext.makeRDD(text)
    dstream
  }
}