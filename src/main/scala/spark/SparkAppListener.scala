package spark

import org.apache.log4j.Logger
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._

import scala.collection.mutable.ArrayBuffer

object SparkAppListener {
  def apply(): SparkAppListener = new SparkAppListener()
}

class SparkAppListener extends SparkListener {
  private val logger = Logger.getLogger(getClass.getName)

  def log(event: String): Unit = logger.info(s"+++ $event")

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = log(s"app: ${event.appName}")

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = log(s"app time: ${event.time}")

  override def onJobStart(event: SparkListenerJobStart): Unit = log(s"job start: ${stageInfos(event.stageInfos)}")

  override def onJobEnd(event: SparkListenerJobEnd): Unit = log(s"job end: ${event.jobResult.toString}")

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = log(s"stage sumbmitted: ${stageInfo(event.stageInfo)}")

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = log(s"stage completed: ${stageInfo(event.stageInfo)}")

  override def onTaskStart(event: SparkListenerTaskStart): Unit = log(s"task start: ${taskInfo(event.taskInfo)}")

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = log(s"task end: ${taskMetrics(event.taskMetrics)}")

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = log(s"task result: ${taskInfo(event.taskInfo)}")

  private def stageInfos(stageInfos: Seq[StageInfo]): String = {
    val info = ArrayBuffer[String]()
    stageInfos.foreach(si => stageInfo(si))
    info.mkString
  }

  private def stageInfo(stageInfo: StageInfo): String = {
    val info = ArrayBuffer[String]()
    info += s"name: ${stageInfo.name} "
    info += s"details: ${stageInfo.details} "
    info.mkString
  }

  private def taskInfo(taskInfo: TaskInfo): String = {
    val info = ArrayBuffer[String]()
    info += s"id: ${taskInfo.taskId} "
    info += s"status: ${taskInfo.status} "
    info.mkString
  }

  private def taskMetrics(taskMetrics: TaskMetrics): String = {
    val metrics = ArrayBuffer[String]()
    metrics += s"executor runtime: ${taskMetrics.executorRunTime} "
    metrics += s"peak-mem-kb: ${taskMetrics.peakExecutionMemory} "
    metrics += s"records: ${taskMetrics.inputMetrics.recordsRead}"
    metrics.mkString
  }
}