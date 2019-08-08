package spark

import org.apache.log4j.Logger
import org.apache.spark.scheduler._

object SparkAppListener {
  def apply(): SparkAppListener = new SparkAppListener()
}

class SparkAppListener extends SparkListener {
  private val logger = Logger.getLogger(getClass.getName)

  def log(event: String): Unit = logger.info(s"+++ $event")

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = log(event.toString)

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = log(event.toString)

  override def onTaskStart(event: SparkListenerTaskStart): Unit = log(event.toString)

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = log(event.toString)

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = log(event.toString)

  override def onJobStart(event: SparkListenerJobStart): Unit = log(event.toString)

  override def onJobEnd(event: SparkListenerJobEnd): Unit = log(event.toString)

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = log(event.toString)

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = log(event.toString)

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = log(event.toString)

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = log(event.toString)

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = log(event.toString)

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = log(event.toString)

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = log(event.toString)

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = log(event.toString)

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = log(event.toString)

  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = log(event.toString)

  override def onExecutorBlacklistedForStage(event: SparkListenerExecutorBlacklistedForStage): Unit = log(event.toString)

  override def onNodeBlacklistedForStage(event: SparkListenerNodeBlacklistedForStage): Unit = log(event.toString)

  override def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit = log(event.toString)

  override def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit = log(event.toString)

  override def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit = log(event.toString)

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = log(event.toString)

  override def onSpeculativeTaskSubmitted(event: SparkListenerSpeculativeTaskSubmitted): Unit = log(event.toString)

  override def onOtherEvent(event: SparkListenerEvent): Unit = log(event.toString)
}