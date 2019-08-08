package spark

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.StreamingQueryListener

object StreamingQueryAppListener {
  def apply(): StreamingQueryAppListener = new StreamingQueryAppListener()
}

class StreamingQueryAppListener extends StreamingQueryListener {
  private val logger = Logger.getLogger(getClass.getName)

  def log(event: String): Unit = logger.info(s"+++ $event")

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = log(event.toString)

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = log(event.toString)

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = log(event.toString)
}