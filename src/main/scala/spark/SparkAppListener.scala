package spark

import org.apache.log4j.Logger
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._

import scala.collection.mutable.ArrayBuffer

object SparkAppListener {
  def apply(): SparkListener = new SparkAppListener()
}

class SparkAppListener extends SparkListener {
  private val events = ArrayBuffer[String]()

  def logEvents(): Unit = {
    val logger = Logger.getLogger(getClass.getName)
    events foreach { event => logger.info(s"*** SparkListener event: $event") }
    println(s"*** SparkListener events: ${events.toString}")
  }

  override def onJobEnd(end: SparkListenerJobEnd): Unit = events += s"+++ ${end.jobResult}"

  override def onTaskEnd(end: SparkListenerTaskEnd): Unit = events += s"+++ Task: ${taskEndToString(end.taskInfo, end.taskMetrics)}"

  private def taskEndToString(taskInfo: TaskInfo, taskMetrics: TaskMetrics): String = {
    val info = ArrayBuffer[String]()
    info += s"${taskInfo.status} "
    info += s"+++ time-ms: ${taskInfo.duration} "
    info += s"+++ mem-kb: ${taskMetrics.peakExecutionMemory} "
    info += s"+++ records: ${taskMetrics.inputMetrics.recordsRead}"
    info.mkString
  }
}