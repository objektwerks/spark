package spark

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.Pattern

import org.apache.spark.sql.Row

object LogEntryParser {
  val logEntryPattern = """(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})? (\S+) (\S+) (\[.+?\]) (.*?) (\d{3}) (\S+) (.*?) (.*?)"""
      .r("ip", "client", "user", "dateTime", "request", "status", "bytes", "referer", "agent")
      .pattern
  val dateTimePattern = Pattern.compile("\\[(.*?) .+]")

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