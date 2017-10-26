package spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import spark.SparkInstance

/**
  * Features: points, country
  * Target: price -> predicted price
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
  val Array(trainingData, testData) = dataframe.randomSplit(Array(0.8, 0.2))

  // Country and country index column.
  val countryColumn = "country"
  val countryIndexColumn = "country_index"

  // Points column.
  val pointsColumn = "points"

  // Features column for points and country index.
  val featuresColumnForPointsAndCountryIndex = "features[points, country_index]"

  // Label column for price.
  val labelColumnForPrice = "price"

  // Prediction column for price.
  val predictionColumnForPrice = s"predicted $labelColumnForPrice"

  // Create country indexer.
  val countryIndexer = new StringIndexer()
    .setInputCol(countryColumn)
    .setOutputCol(countryIndexColumn)

  // Create points and country index features vector.
  val featuresVector = new VectorAssembler()
    .setInputCols(Array(pointsColumn, countryIndexColumn))
    .setOutputCol(featuresColumnForPointsAndCountryIndex)

  // Create GBT regressor - or gradient-boosted tree estimator.
  val gradientBoostedTreeEstimator = new GBTRegressor()
    .setLabelCol(labelColumnForPrice)
    .setFeaturesCol(featuresColumnForPointsAndCountryIndex)
    .setPredictionCol(predictionColumnForPrice)
    .setMaxIter(33)

  // Create stages and pipeline.
  val stages = Array(countryIndexer, featuresVector, gradientBoostedTreeEstimator)
  val pipeline = new Pipeline().setStages(stages)

  // Create model via pipeline and training dataset.
  val model = pipeline.fit(trainingData)

  // Create predictions dataframe via model and test dataset.
  val predictions = model.transform(testData)
  predictions.createOrReplaceTempView("predictions")
  sqlContext.sql("select * from predictions order by points desc").show(10)

  // Create regression evaluator.
  val evaluator = new RegressionEvaluator()
    .setLabelCol(labelColumnForPrice)
    .setPredictionCol(predictionColumnForPrice)
    .setMetricName("rmse")

  // Evaluate predictions via regression evaluator.
  println(s"Regression Root Mean Squared Error: ${evaluator.evaluate(predictions)}")

  sparkListener.log()
  sparkSession.stop()
}