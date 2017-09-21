package spark

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.io.Source

class MLibTest extends FunSuite {
  import SparkInstance._

  test("kmeans") {
    val streamingContext = new StreamingContext(sparkContext, batchDuration = Milliseconds(100))

    val kmeansTrainingDStream = textFileToDStream("/kmeans-training.txt", streamingContext)
    val kmeansTestingDStream = textFileToDStream("/kmeans-testing.txt", streamingContext)

    val kmeansTrainingData = kmeansTrainingDStream.map(Vectors.parse).cache()
    val kmeansTestingData = kmeansTestingDStream.map(LabeledPoint.parse)

    kmeansTrainingData.print()

    val model = new StreamingKMeans()
      .setK(5)
      .setDecayFactor(1.0)
      .setRandomCenters(2, 0.0)

    model.trainOn(kmeansTrainingData)
    model.predictOnValues(kmeansTestingData.map(labeledPoint => (labeledPoint.label.toInt, labeledPoint.features))).print()

    streamingContext.checkpoint("./target/output/test/kmeans/checkpoint")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  test("regression") {
    val streamingContext = new StreamingContext(sparkContext, batchDuration = Milliseconds(100))

    val regressionTrainingDStream = textFileToDStream("/regression.txt", streamingContext)
    val regressionTestingDStream = textFileToDStream("/regression.txt", streamingContext)

    val regressionTrainingData = regressionTrainingDStream.map(LabeledPoint.parse).cache()
    val regressionTestingData = regressionTestingDStream.map(LabeledPoint.parse)

    regressionTrainingData.print()

    val numFeatures = 1
    val model = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.zeros(numFeatures))
    model.algorithm.setIntercept(true)

    model.trainOn(regressionTrainingData)
    model.predictOnValues(regressionTestingData.map(labeledPoint => (labeledPoint.label, labeledPoint.features))).print()

    streamingContext.checkpoint("./target/output/test/regression/checkpoint")
    streamingContext.start
    streamingContext.awaitTerminationOrTimeout(100)
    streamingContext.stop(stopSparkContext = false, stopGracefully = true)
  }

  def textFileToDStream(filePath: String, streamingContext: StreamingContext): InputDStream[String] = {
    val text = Source.fromInputStream(getClass.getResourceAsStream(filePath)).getLines.toSeq
    val queue = mutable.Queue[RDD[String]]()
    val dstream = streamingContext.queueStream(queue)
    queue += sparkContext.makeRDD(text)
    dstream
  }
}