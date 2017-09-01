package spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.io.Codec

object LogEntryApp extends App {
  val sparkSession = SparkSession.builder.master("local[2]").appName("sparky").getOrCreate()

  import LogEntry._
  import LogEntryParser._
  import sparkSession.implicits._

  val logger = Logger.getLogger(this.getClass)

  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  val reader = sparkSession.readStream.text("./data/log")
  val logEntries = reader.flatMap(parseRow)
  val writer = logEntries.writeStream.foreach(logEntryForeachWriter)
  val query = writer.start()
  query.awaitTermination(60000)
  sparkSession.stop()
}