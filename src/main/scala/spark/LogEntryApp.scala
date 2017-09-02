package spark

import java.nio.charset.CodingErrorAction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

import scala.io.Codec

object LogEntryApp extends App {
  val sparkSession = SparkSession.builder.master("local[2]").appName("sparky").getOrCreate()

  import LogEntry._
  import sparkSession.implicits._

  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
  val reader = sparkSession.readStream.text("./data/log")

  val logEntries = reader
    .flatMap(rowToLogEntry)
    .select("status", "dateTime", "ip")
    .withWatermark("dateTime", "10 minutes")
    .groupBy($"ip", $"status", window($"dateTime", "1 hour"))
    .count
    .orderBy("window")

  val writer = logEntries
    .writeStream
    .outputMode("complete")
    .foreach(rowForeachWriter)
    .start
  writer.awaitTermination(60000)

  sparkSession.stop
}