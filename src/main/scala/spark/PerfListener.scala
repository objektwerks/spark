package spark

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

class PerfListener extends SparkListener {
  var totalExecutorRunTime = 0L
  var jvmGCTime = 0L
  var recordsRead = 0L
  var recordsWritten = 0L
  var resultSerializationTime = 0L /** * Called when a task ends */

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    updateMetricsForTask(metrics)
  }

  def updateMetricsForTask(metrics: TaskMetrics): Unit = {
    totalExecutorRunTime += metrics.executorRunTime
    jvmGCTime += metrics.jvmGCTime
    resultSerializationTime += metrics.resultSerializationTime
    recordsRead += metrics.inputMetrics.recordsRead
    recordsWritten += metrics.outputMetrics.recordsWritten
  }
}