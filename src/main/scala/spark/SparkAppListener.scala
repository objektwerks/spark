package spark

import org.apache.spark.scheduler._

class SparkAppListener extends SparkListener {
  override def onApplicationStart(start: SparkListenerApplicationStart): Unit = println(s"On app start: ${start.toString}")

  override def onApplicationEnd(end: SparkListenerApplicationEnd): Unit = println(s"On app end: ${end.toString}")

  override def onJobStart(start: SparkListenerJobStart): Unit = println(s"On job start: ${start.toString}")

  override def onJobEnd(end: SparkListenerJobEnd): Unit = println(s"On job end: ${end.toString}")

  override def onStageSubmitted(submitted: SparkListenerStageSubmitted): Unit = println(s"On stage submitted: ${submitted.toString}")

  override def onStageCompleted(completed: SparkListenerStageCompleted): Unit = println(s"On stage completed: ${completed.toString}")

  override def onExecutorMetricsUpdate(update: SparkListenerExecutorMetricsUpdate): Unit = println(s"On executor update: ${update.toString}")

  override def onExecutorAdded(added: SparkListenerExecutorAdded): Unit = println(s"On executor added: ${added.toString}")

  override def onExecutorRemoved(removed: SparkListenerExecutorRemoved): Unit = println(s"On executor removed: ${removed.toString}")

  override def onExecutorBlacklisted(blacklisted: SparkListenerExecutorBlacklisted): Unit = println(s"On executor blacklisted: ${blacklisted.toString}")

  override def onExecutorUnblacklisted(unblacklisted: SparkListenerExecutorUnblacklisted): Unit = println(s"On executor unblacklisted: ${unblacklisted.toString}")

  override def onBlockManagerAdded(added: SparkListenerBlockManagerAdded): Unit = println(s"On block manager added: ${added.toString}")

  override def onBlockManagerRemoved(removed: SparkListenerBlockManagerRemoved): Unit = println(s"On block manager removed: ${removed.toString}")

  override def onBlockUpdated(updated: SparkListenerBlockUpdated): Unit = println(s"On block updated: ${updated.toString}")

  override def onTaskStart(start: SparkListenerTaskStart) = println(s"On task start: ${start.toString}")

  override def onTaskEnd(end: SparkListenerTaskEnd) = println(s"On task end: ${end.toString}")

  override def onTaskGettingResult(result: SparkListenerTaskGettingResult): Unit = println(s"On task result: ${result.toString}")

  override def onUnpersistRDD(unpersist: SparkListenerUnpersistRDD): Unit = println(s"On rdd unpersisted: ${unpersist.toString}")

  override def onNodeBlacklisted(blacklisted: SparkListenerNodeBlacklisted): Unit = println(s"On node blacklisted: ${blacklisted.toString}")

  override def onNodeUnblacklisted(unblacklisted: SparkListenerNodeUnblacklisted): Unit = println(s"On node unblacklisted: ${unblacklisted.toString}")

  override def onEnvironmentUpdate(update: SparkListenerEnvironmentUpdate): Unit = println(s"On env update: ${update.toString}")

  override def onOtherEvent(event: SparkListenerEvent): Unit = println(s"On other event: ${event.toString}")
}