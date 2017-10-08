package spark

import org.apache.log4j.Logger
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._

import scala.collection.mutable.ListBuffer

class SparkAppListener extends SparkListener {
  val events = ListBuffer[String]()

  def log(): Unit = {
    val logger = Logger.getLogger("SparkAppListener")
    events foreach { event => logger.info(event) }
  }

  override def onJobEnd(end: SparkListenerJobEnd): Unit = events += s"*** Job result - ${end.jobResult}"

  override def onTaskEnd(end: SparkListenerTaskEnd): Unit = events += s"*** Task result - ${taskInfoToString(end.taskInfo)} ${taskMetricsToString(end.taskMetrics)}"

  def taskInfoToString(taskInfo: TaskInfo): String = {
    val info = ListBuffer[String]()
    info += s"status: ${taskInfo.status} "
    info += s"duration: ${taskInfo.duration} "
    info.mkString
  }

  def taskMetricsToString(taskMetrics: TaskMetrics): String = {
    val info = ListBuffer[String]()
    info += s"executor run time: ${taskMetrics.executorRunTime} "
    info += s"peak execution memory: ${taskMetrics.peakExecutionMemory} "
    info += s"result size: ${taskMetrics.resultSize}"
    info.mkString
  }
}