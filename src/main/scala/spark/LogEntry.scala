package spark

import org.apache.spark.sql.ForeachWriter

case class LogEntry(ip: String,
                    client: String,
                    user: String,
                    dateTime: Option[String],
                    request: String,
                    status: String,
                    bytes: String,
                    referer: String,
                    agent: String)

object LogEntry {
  val logEntryForeachWriter = new ForeachWriter[LogEntry] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(logEntry: LogEntry): Unit = println(logEntry)
    override def close(errorOrNull: Throwable): Unit = println("Closing logEnty foreach writer...")
  }
}