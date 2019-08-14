package spark.streaming

import java.nio.charset.CodingErrorAction

import org.apache.spark.sql.functions.window
import spark.SparkInstance

import scala.io.Codec

object LogEntryApp extends App {
  import LogEntry._
  import SparkInstance._
  import sparkSession.implicits._

  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  sparkSession
    .readStream
    .text("./data/log")
    .flatMap(rowToLogEntry)
    .select("status", "dateTime", "ip")
    .withWatermark("dateTime", "1 minute")
    .groupBy($"status", $"ip", window($"dateTime", "15 seconds"))
    .count
    .orderBy("window")
    .writeStream
    .outputMode("complete")
    .foreach(rowForeachWriter)
    .start
    .awaitTermination
}