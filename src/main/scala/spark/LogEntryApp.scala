package spark

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.Pattern

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.{Row, SparkSession}

case class LogEntry(ip: String,
                    client: String,
                    user: String,
                    dateTime: Option[String],
                    request: String,
                    status: String,
                    bytes: String,
                    referer: String,
                    agent: String)

object LogEntryParser {
  val logEntryPattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }
  val dateTimePattern = Pattern.compile("\\[(.*?) .+]")

  println(s"log entry pattern: ${logEntryPattern.toString}")
  println(s"date time pattern: ${dateTimePattern.toString}")

  def parseRow(row: Row): Option[LogEntry] = {
    val matcher = logEntryPattern.matcher(row.getString(0))
    if (matcher.matches()) {
      Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateTime(matcher.group(4)),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)))
    } else None
  }

  def parseDateTime(dateTime: String): Option[String] = {
    val dateTimeMatcher = dateTimePattern.matcher(dateTime)
    if (dateTimeMatcher.find) {
      val dateTimeAsString = dateTimeMatcher.group(1)
      val dateTimeFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = dateTimeFormat.parse(dateTimeAsString)
      val timestamp = new Timestamp(date.getTime)
      Some(timestamp.toString)
    } else None
  }

}

object LogEntryApp extends App {
  val sparkSession = SparkSession.builder
    .master("local[2]")
    .appName("sparky")
    .getOrCreate()

  import LogEntryParser._
  import sparkSession.implicits._

  val logs = sparkSession.readStream.text("./data/log")
  val logEntries = logs.flatMap(parseRow).select("status", "dateTime")
  val dataset = logEntries.groupBy($"status", window($"dateTime", "1 hour")).count().orderBy("window")
  val writer = dataset.writeStream.outputMode("complete").format("console")
  val query = writer.start()
  query.awaitTermination()
  sparkSession.stop()
}