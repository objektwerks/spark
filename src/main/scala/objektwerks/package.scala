import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable
import scala.util.Try

package object objektwerks {
  def createSparkEventsDir(dir: String): Boolean = {
    import java.nio.file.{Files, Paths}
    val path = Paths.get(dir)
    if (!Files.exists(path))
      Try ( Files.createDirectories(path) ).isSuccess
    else true
  }

  def textFileToDStream(filePath: String, sparkContext: SparkContext, streamingContext: StreamingContext): DStream[String] = {
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    val lines = sparkContext.textFile(filePath)
    queue += lines
    dstream
  }

  def textToDStream(filePath: String, streamingContext: StreamingContext): DStream[String] = {
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    val lines = SparkInstance.sparkContext.textFile(filePath)
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

  def countWords(ds: DStream[String], windowLengthInMillis: Long, slideIntervalInMillis: Long): DStream[(String, Int)] = {
    ds.flatMap(line => line.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKeyAndWindow((x:Int, y:Int) => x + y, Milliseconds(windowLengthInMillis), Milliseconds(slideIntervalInMillis))
  }
}