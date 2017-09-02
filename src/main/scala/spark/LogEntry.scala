package spark

import org.apache.log4j.Logger
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
  val logger = Logger.getLogger(this.getClass)

  val logEntryForeachWriter = new ForeachWriter[LogEntry] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(logEntry: LogEntry): Unit = logger.info(logEntry)
    override def close(errorOrNull: Throwable): Unit = logger.info("Closing logEntry foreach writer...")
  }
}