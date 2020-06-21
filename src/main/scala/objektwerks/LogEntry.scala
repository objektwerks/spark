package objektwerks

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.regex.Pattern

import org.apache.spark.sql.{ForeachWriter, Row}

case class LogEntry(ip: String,
                    client: String,
                    user: String,
                    dateTime: Option[Timestamp],
                    request: String,
                    status: String,
                    bytes: String,
                    referer: String,
                    agent: String)

object LogEntry {
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
  val rowForeachWriter = new ForeachWriter[Row] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(row: Row): Unit = println(s"$row")
    override def close(errorOrNull: Throwable): Unit = println("Closing row foreach writer...")
  }
  val dateTimePattern = Pattern.compile("\\[(.*?) .+]")
  val dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)

  def rowToLogEntry(row: Row): Option[LogEntry] = {
    val matcher = logEntryPattern.matcher(row.getString(0))
    if (matcher.matches()) {
      Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        dateTimeToTimestamp(matcher.group(4)),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)))
    } else None
  }

  def dateTimeToTimestamp(dateTime: String): Option[Timestamp] = {
    val dateTimeMatcher = dateTimePattern.matcher(dateTime)
    if (dateTimeMatcher.find) {
      val dateTimeAsString = dateTimeMatcher.group(1)
      val dateTime = LocalDateTime.parse(dateTimeAsString, dateTimeFormatter)
      val timestamp = Timestamp.valueOf(dateTime)
      Some(timestamp)
    } else None
  }
}