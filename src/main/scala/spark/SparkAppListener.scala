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

  override def onApplicationStart(start: SparkListenerApplicationStart): Unit = events += s"*** On app start: ${start.toString}"

  override def onApplicationEnd(end: SparkListenerApplicationEnd): Unit = events += s"*** On app end: ${end.toString}"

  override def onJobStart(start: SparkListenerJobStart): Unit = events += s"*** On job start: ${start.toString}"

  override def onJobEnd(end: SparkListenerJobEnd): Unit = events += s"*** On job end: ${end.toString}"

  override def onStageSubmitted(submitted: SparkListenerStageSubmitted): Unit = events += s"*** On stage submitted: ${submitted.toString}"

  override def onStageCompleted(completed: SparkListenerStageCompleted): Unit = events += s"*** On stage completed: ${completed.toString}"

  override def onExecutorMetricsUpdate(update: SparkListenerExecutorMetricsUpdate): Unit = events += s"*** On executor update: ${update.toString}"

  override def onTaskEnd(end: SparkListenerTaskEnd): Unit = {
    events += s"*** On task end - task info: ${taskInfoToString(end.taskInfo)}"
    events += s"*** On task end - task metrics: ${taskMetricsToString(end.taskMetrics)}"
  }

  def taskInfoToString(taskInfo: TaskInfo): String = {
    val info = ListBuffer[String]()
    info += s"host: ${taskInfo.host}"
    info += s"executor id: ${taskInfo.executorId}"
    info += s"task id: ${taskInfo.taskId}"
    info += s"running?: ${taskInfo.running}"
    info += s"status: ${taskInfo.status}"
    info += s"attempt number: ${taskInfo.attemptNumber}"
    info += s"launch time: ${taskInfo.launchTime}"
    info += s"finish time: ${taskInfo.finishTime}"
    info += s"result time: ${taskInfo.gettingResultTime}"
    info += s"duration: ${taskInfo.duration}"
    info += s"getting result?: ${taskInfo.gettingResult}"
    info += s"successful?: ${taskInfo.successful}"
    info += s"failed?: ${taskInfo.failed}"
    info += s"killed?: ${taskInfo.killed}"
    info += s"accumuables: ${taskInfo.accumulables.foreach(println)}"
    info.mkString
  }

  def taskMetricsToString(taskMetrics: TaskMetrics): String = {
    val info = ListBuffer[String]()
    info += s"executor cpu time: ${taskMetrics.executorCpuTime}"
    info += s"result size: ${taskMetrics.resultSize}"
    info.mkString
  }
}