import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.mutable
import scala.io.Source

package object spark {
  def textFileToDStream(filePath: String, sparkContext: SparkContext, streamingContext: StreamingContext): InputDStream[String] = {
    val text = Source.fromInputStream(getClass.getResourceAsStream(filePath)).getLines.toSeq
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    queue += sparkContext.makeRDD(text)
    dstream
  }
}