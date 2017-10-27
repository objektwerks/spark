package spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import spark.SparkInstance

/**
  * Features: points, variety, country, province, region
  * Prediction: price
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
  val pointsColumn = "points"
  val varietyColumn = "variety"
  val varietyIndexColumn = "variety_idx"
  val countryColumn = "country"
  val countryIndexColumn = "country_idx"
  val provinceColumn = "province"
  val provinceIndexColumn = "province_idx"
  val regionColumn = "region_2"
  val regionIndexColumn = "region_2_idx"
  val priceColumn = "price"
  val featuresColumn = s"features[points, variety, country, province, region]"
  val predictionColumn = "prediction[price]"

  // Variety indexer.
  val varietyIndexer = new StringIndexer()
    .setInputCol(varietyColumn)
    .setOutputCol(varietyIndexColumn)
    .setHandleInvalid("keep")

  // Country indexer.
  val countryIndexer = new StringIndexer()
    .setInputCol(countryColumn)
    .setOutputCol(countryIndexColumn)

  // Province indexer.
  val provinceIndexer = new StringIndexer()
    .setInputCol(priceColumn)
    .setOutputCol(provinceIndexColumn)
    .setHandleInvalid("keep")

  // Region indexer.
  val regionIndexer = new StringIndexer()
    .setInputCol(regionColumn)
    .setOutputCol(regionIndexColumn)
    .setHandleInvalid("keep")

  // Features vector.
  val featuresVector = new VectorAssembler()
    .setInputCols(Array(pointsColumn, varietyIndexColumn, countryIndexColumn, provinceIndexColumn, regionIndexColumn))
    .setOutputCol(featuresColumn)

  // Regression.
  val gradientBoostedTreeRegressor = new GBTRegressor()
    .setLabelCol(priceColumn)
    .setFeaturesCol(featuresColumn)
    .setPredictionCol(predictionColumn)
    .setMaxBins(176)
    .setMaxIter(10)

  // Pipeline.
  val stages = Array(varietyIndexer, countryIndexer, provinceIndexer, regionIndexer, featuresVector, gradientBoostedTreeRegressor)
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
  println(s"Root Mean Squared Error in terms of Price Deviation: ${evaluator.evaluate(predictions)}")

  sparkListener.log()
  sparkSession.stop()
}