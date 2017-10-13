package spark.mlib

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.{SparkInstance, textFileToDStream}

object KMeansApp extends App {
  import SparkInstance._
  val streamingContext = new StreamingContext(sparkContext, batchDuration = Seconds(1))

  val kmeansTrainingDStream = textFileToDStream("./data/txt/kmeans-training.txt", sparkContext, streamingContext)
  val kmeansTestingDStream = textFileToDStream("./data/txt/kmeans-testing.txt", sparkContext, streamingContext)

  val kmeansTrainingData = kmeansTrainingDStream.map(Vectors.parse).cache()
  val kmeansTestingData = kmeansTestingDStream.map(LabeledPoint.parse)

  kmeansTrainingData.print()

  val model = new StreamingKMeans()
    .setK(5)
    .setDecayFactor(1.0)
    .setRandomCenters(2, 0.0)

  model.trainOn(kmeansTrainingData)
  model.predictOnValues(kmeansTestingData.map(labeledPoint => (labeledPoint.label.toInt, labeledPoint.features))).print()

  streamingContext.start
  streamingContext.awaitTerminationOrTimeout(1000)
  streamingContext.stop(stopSparkContext = false, stopGracefully = true)

  sparkListener.log()
  sparkSession.stop()
}