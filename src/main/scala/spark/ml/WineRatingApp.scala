package spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import spark.SparkInstance

object WineRatingApp extends App {
  import SparkInstance._
  import Wine._

  // Data.
  val dataframe = sparkSession.read.format("csv").option("header", "true").schema(wineSchema).load("./data/wine/*.csv").na.drop()
  val Array(trainingData, testData) = dataframe.randomSplit(Array(0.8, 0.2))

  // Label.
  val labelColumn = "price"

  // Category Indexer.
  val countryIndexer = new StringIndexer()
    .setInputCol("country")
    .setOutputCol("countryIndex")

  // Features assembler.
  val featuresAssembler = new VectorAssembler()
    .setInputCols(Array("points", "countryIndex"))
    .setOutputCol("features")

  // Estimator - gradient-boosted tree estimator.
  val gradientBoostedTreeEstimator = new GBTRegressor()
    .setLabelCol(labelColumn)
    .setFeaturesCol("features")
    .setPredictionCol("Predicted " + labelColumn)
    .setMaxIter(50)

  // Stages.
  val stages = Array(countryIndexer, featuresAssembler, gradientBoostedTreeEstimator)

  // Pipeline.
  val pipeline = new Pipeline().setStages(stages)

  // Model.
  val model = pipeline.fit(trainingData)

  // Predictions.
  val predictions = model.transform(testData)

  // Evaluator - evaluate the error/deviation of the regression using the Root Mean Squared deviation.
  val evaluator = new RegressionEvaluator()
    .setLabelCol(labelColumn)
    .setPredictionCol("Predicted " + labelColumn)
    .setMetricName("rmse")

  // Evaluate!
  val error = evaluator.evaluate(predictions)
  println(error)

  sparkListener.log()
  sparkSession.stop()
}