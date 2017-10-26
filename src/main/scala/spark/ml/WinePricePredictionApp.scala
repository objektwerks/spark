package spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import spark.SparkInstance

/**
  * Features: points, price
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

  // Columns.
  val pointsColumn = "points"
  val priceColumn = "price"
  val featuresColumn = s"features[$pointsColumn, $priceColumn]"
  val predictionColumn = s"$priceColumn increase prediction"

  // Features vector.
  val featuresVector = new VectorAssembler()
    .setInputCols(Array(pointsColumn, priceColumn))
    .setOutputCol(featuresColumn)

  // Gradient-boosted tree regressor.
  val gradientBoostedTreeRegressor = new GBTRegressor()
    .setLabelCol(priceColumn)
    .setFeaturesCol(featuresColumn)
    .setPredictionCol(predictionColumn)
    .setMaxIter(10)

  // Create stages and pipeline.
  val stages = Array(featuresVector, gradientBoostedTreeRegressor)
  val pipeline = new Pipeline().setStages(stages)

  // Create model via pipeline and training dataset.
  val model = pipeline.fit(trainingDataset)

  // Create predictions dataframe via model and test dataset.
  val predictions = model.transform(testDataset)
  predictions.createOrReplaceTempView("price_increase_predictions")
  sqlContext.sql("select * from price_increase_predictions order by price desc").show(10)

  // Create regression evaluator.
  val evaluator = new RegressionEvaluator()
    .setLabelCol(priceColumn)
    .setPredictionCol(predictionColumn)
    .setMetricName("rmse")

  // Evaluate predictions via regression evaluator.
  println(s"Root Mean Squared Error in terms of Price Increase: ${evaluator.evaluate(predictions)}")

  sparkListener.log()
  sparkSession.stop()
}