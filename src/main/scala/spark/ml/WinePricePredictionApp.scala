package spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import spark.SparkInstance

/**
  * Features: country, points
  * Prediction: price increase
  */
object WinePricePredictionApp extends App {
  import SparkInstance._
  import Wine._

  // Data.
  val dataframe = sparkSession
    .read
    .schema(wineSchema)
    .option("header", "true")
    .csv("./data/wine/*.csv")
    .na
    .drop()

  // Training and Test datasets.
  val Array(trainingDataset, testDataset) = dataframe.randomSplit(Array(0.8, 0.2))

  // Columns.
  val countryColumn = "country"
  val countryIndexColumn = "country_index"
  val pointsColumn = "points"
  val priceColumn = "price"
  val featuresColumn = s"features[$countryIndexColumn, $pointsColumn]"
  val predictionColumn = "prediction[price increase]"

  // Country indexer.
  val countryIndexer = new StringIndexer()
    .setInputCol(countryColumn)
    .setOutputCol(countryIndexColumn)

  // Features vector.
  val featuresVector = new VectorAssembler()
    .setInputCols(Array(countryIndexColumn, pointsColumn))
    .setOutputCol(featuresColumn)

  // Regression.
  val gradientBoostedTreeRegressor = new GBTRegressor()
    .setLabelCol(priceColumn)
    .setFeaturesCol(featuresColumn)
    .setPredictionCol(predictionColumn)
    .setMaxIter(10)

  // Pipeline.
  val stages = Array(countryIndexer, featuresVector, gradientBoostedTreeRegressor)
  val pipeline = new Pipeline().setStages(stages)

  // Model.
  val model = pipeline.fit(trainingDataset)

  // Predictions.
  val predictions = model.transform(testDataset)
  predictions.createOrReplaceTempView("price_increase_predictions")
  sqlContext.sql("select * from price_increase_predictions order by price desc").show(10)

  // Evaluate.
  val evaluator = new RegressionEvaluator()
    .setLabelCol(priceColumn)
    .setPredictionCol(predictionColumn)
    .setMetricName("rmse")
  println(s"Root Mean Squared Error in terms of Price Increase: ${evaluator.evaluate(predictions)}")

  sparkListener.log()
  sparkSession.stop()
}