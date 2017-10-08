package spark

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, TaskInfo}

import scala.collection.mutable.ListBuffer

class SparkTaskListener extends SparkListener {
  var taskInfos = ListBuffer[TaskInfo]()
  var totalExecutorRunTime = 0L
  var jvmGCTime = 0L
  var recordsRead = 0L
  var recordsWritten = 0L
  var resultSerializationTime = 0L

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = update(taskEnd.taskInfo, taskEnd.taskMetrics)

  private def update(taskInfo: TaskInfo, taskMetrics: TaskMetrics): Unit = {
    totalExecutorRunTime += taskMetrics.executorRunTime
    jvmGCTime += taskMetrics.jvmGCTime
    resultSerializationTime += taskMetrics.resultSerializationTime
    recordsRead += taskMetrics.inputMetrics.recordsRead
    recordsWritten += taskMetrics.outputMetrics.recordsWritten
    taskInfos += taskInfo
  }
}