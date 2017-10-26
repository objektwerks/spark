package spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import spark.SparkInstance

/**
  * Features: points
  * Label: price
  * Target: price increase
  */
object WinePricePredictionApp extends App {
  import SparkInstance._
  import Wine._

  // Load data files into dataframe.
  val dataframe = sparkSession
    .read
    .format("csv")
    .option("header", "true")
    .schema(wineSchema)
    .load("./data/wine/*.csv")
    .na
    .drop()

  // Split dataframe into training and test datasets.
  val Array(trainingDataset, testDataset) = dataframe.randomSplit(Array(0.8, 0.2))

  // Points column.
  val pointsColumn = "points"

  // Features column for points.
  val featuresColumnForPoints = "features[points]"

  // Label column for price.
  val labelColumnForPrice = "price"

  // Prediction column for price increase.
  val predictionColumnForPriceIncrease = s"predicted $labelColumnForPrice increase"

  // Create points and country index features vector.
  val featuresVector = new VectorAssembler()
    .setInputCols(Array(pointsColumn))
    .setOutputCol(featuresColumnForPoints)

  // Create GBT regressor - or gradient-boosted tree estimator.
  val gradientBoostedTreeEstimator = new GBTRegressor()
    .setLabelCol(labelColumnForPrice)
    .setFeaturesCol(featuresColumnForPoints)
    .setPredictionCol(predictionColumnForPriceIncrease)
    .setMaxIter(10)

  // Create stages and pipeline.
  val stages = Array(featuresVector, gradientBoostedTreeEstimator)
  val pipeline = new Pipeline().setStages(stages)

  // Create model via pipeline and training dataset.
  val model = pipeline.fit(trainingDataset)

  // Create predictions dataframe via model and test dataset.
  val predictions = model.transform(testDataset)
  predictions.createOrReplaceTempView("price_increase_predictions")
  sqlContext.sql("select * from price_increase_predictions order by price desc").show(10)

  // Create regression evaluator.
  val evaluator = new RegressionEvaluator()
    .setLabelCol(labelColumnForPrice)
    .setPredictionCol(predictionColumnForPriceIncrease)
    .setMetricName("rmse")

  // Evaluate predictions via regression evaluator.
  println(s"Root Mean Squared Error in terms of Price Increase: ${evaluator.evaluate(predictions)}")

  sparkListener.log()
  sparkSession.stop()
}