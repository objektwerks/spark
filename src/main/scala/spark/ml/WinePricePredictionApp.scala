package spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import spark.SparkInstance

object WinePricePredictionApp extends App {
  import SparkInstance._
  import Wine._

  // Dataframe.
  val dataframe = sparkSession
    .read
    .format("csv")
    .option("header", "true")
    .schema(wineSchema)
    .load("./data/wine/*.csv")
    .na
    .drop()

  // Training and Test Datasets.
  val Array(trainingData, testData) = dataframe.randomSplit(Array(0.8, 0.2))

  // Country Indexer.
  val countryIndexer = new StringIndexer()
    .setInputCol("country")
    .setOutputCol("countryIndex")

  // Points and CountryIndex Features.
  val featuresAssembler = new VectorAssembler()
    .setInputCols(Array("points", "countryIndex"))
    .setOutputCol("features")

  // Label.
  val labelColumn = "price"

  // Estimator - gradient-boosted tree estimator.
  val gradientBoostedTreeEstimator = new GBTRegressor()
    .setLabelCol(labelColumn)
    .setFeaturesCol("features")
    .setPredictionCol("Predicted " + labelColumn)
    .setMaxIter(50)

  // Pipeline.
  val stages = Array(countryIndexer, featuresAssembler, gradientBoostedTreeEstimator)
  val pipeline = new Pipeline().setStages(stages)

  // Model.
  val model = pipeline.fit(trainingData)

  // Predictions Dataframe.
  val predictions = model.transform(testData)
  predictions.show(10)

  // Predictions Evaluator.
  val evaluator = new RegressionEvaluator()
    .setLabelCol(labelColumn)
    .setPredictionCol("Predicted " + labelColumn)
    .setMetricName("rmse")
  println(s"Regression Root Mean Squared Deviation: ${evaluator.evaluate(predictions)}")

  sparkListener.log()
  sparkSession.stop()
}