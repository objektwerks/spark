package spark

import java.nio.charset.CodingErrorAction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

import scala.io.Codec

object LogEntryApp extends App {
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
  val sparkSession = SparkSession.builder.master("local[2]").appName("sparky").getOrCreate()

  import LogEntry._
  import LogEntryParser._
  import sparkSession.implicits._

  val reader = sparkSession.readStream.text("./data/log")
  val logEntries = reader.flatMap(parseRow).cache

  val statusDateTime = logEntries
    .select("status", "dateTime")
    .groupBy($"status", window($"dateTime", "1 hour"))
    .count
    .orderBy("window")
  val statusDateTimeWriter = statusDateTime
    .writeStream
    .outputMode("complete")
    .format("console")
    .start

  val logEntryWriter = logEntries
    .writeStream
    .foreach(logEntryForeachWriter)
    .start

  statusDateTimeWriter.awaitTermination(60000)
  logEntryWriter.awaitTermination(60000)

  sparkSession.stop
}