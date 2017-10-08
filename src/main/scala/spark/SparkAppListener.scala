package spark

import org.apache.spark.scheduler._

class SparkAppListener extends SparkListener {
  override def onApplicationStart(start: SparkListenerApplicationStart): Unit = super.onApplicationStart(start)

  override def onApplicationEnd(end: SparkListenerApplicationEnd): Unit = super.onApplicationEnd(end)

  override def onJobStart(start: SparkListenerJobStart): Unit = super.onJobStart(start)

  override def onJobEnd(end: SparkListenerJobEnd): Unit = super.onJobEnd(end)

  override def onStageSubmitted(submitted: SparkListenerStageSubmitted): Unit = super.onStageSubmitted(submitted)

  override def onStageCompleted(completed: SparkListenerStageCompleted): Unit = super.onStageCompleted(completed)

  override def onExecutorMetricsUpdate(update: SparkListenerExecutorMetricsUpdate): Unit = super.onExecutorMetricsUpdate(update)

  override def onExecutorAdded(added: SparkListenerExecutorAdded): Unit = super.onExecutorAdded(added)

  override def onExecutorRemoved(removed: SparkListenerExecutorRemoved): Unit = super.onExecutorRemoved(removed)

  override def onExecutorBlacklisted(blacklisted: SparkListenerExecutorBlacklisted): Unit = super.onExecutorBlacklisted(blacklisted)

  override def onExecutorUnblacklisted(unblacklisted: SparkListenerExecutorUnblacklisted): Unit = super.onExecutorUnblacklisted(unblacklisted)

  override def onBlockManagerAdded(added: SparkListenerBlockManagerAdded): Unit = super.onBlockManagerAdded(added)

  override def onBlockManagerRemoved(removed: SparkListenerBlockManagerRemoved): Unit = super.onBlockManagerRemoved(removed)

  override def onBlockUpdated(updated: SparkListenerBlockUpdated): Unit = super.onBlockUpdated(updated)

  override def onTaskStart(start: SparkListenerTaskStart) = println(s"On task start: ${start.toString}")

  override def onTaskEnd(end: SparkListenerTaskEnd) = println(s"On task end: ${end.toString}")

  override def onTaskGettingResult(result: SparkListenerTaskGettingResult): Unit = super.onTaskGettingResult(result)

  override def onUnpersistRDD(unpersist: SparkListenerUnpersistRDD): Unit = super.onUnpersistRDD(unpersist)

  override def onNodeBlacklisted(blacklisted: SparkListenerNodeBlacklisted): Unit = super.onNodeBlacklisted(blacklisted)

  override def onNodeUnblacklisted(unblacklisted: SparkListenerNodeUnblacklisted): Unit = super.onNodeUnblacklisted(unblacklisted)

  override def onEnvironmentUpdate(update: SparkListenerEnvironmentUpdate): Unit = super.onEnvironmentUpdate(update)

  override def onOtherEvent(event: SparkListenerEvent): Unit = super.onOtherEvent(event)
}