package spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import spark.SparkInstance

/**
  * Features: points, variety, province, region
  * Prediction: price
  *
  * Credit: Marcin Gorczynski ( https://blog.scalac.io/scala-spark-ml.html ) for the foundation of this app!
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
  val Array(trainingData, testData) = dataframe.randomSplit(Array(0.8, 0.2))
  val trainingDataset = trainingData.cache
  val testDataset = testData.cache

  // Columns.
  val pointsColumn = "points"
  val varietyColumn = "variety"
  val varietyIndexColumn = "variety_idx"
  val provinceColumn = "province"
  val provinceIndexColumn = "province_idx"
  val regionColumn = "region_1"
  val regionIndexColumn = "region_1_idx"
  val priceColumn = "price"
  val featuresColumn = "features[points, variety, province, region]"
  val predictionColumn = "prediction[price]"

  // Variety indexer.
  val varietyIndexer = new StringIndexer()
    .setInputCol(varietyColumn)
    .setOutputCol(varietyIndexColumn)
    .setHandleInvalid("keep")

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
    .setInputCols(Array(pointsColumn, varietyIndexColumn, provinceIndexColumn, regionIndexColumn))
    .setOutputCol(featuresColumn)

  // Regression.
  val gradientBoostedTreeRegressor = new GBTRegressor()
    .setLabelCol(priceColumn)
    .setFeaturesCol(featuresColumn)
    .setPredictionCol(predictionColumn)
    .setMaxBins(209)
    .setMaxIter(10)

  // Pipeline.
  val stages = Array(varietyIndexer, provinceIndexer, regionIndexer, featuresVector, gradientBoostedTreeRegressor)
  val pipeline = new Pipeline().setStages(stages)

  // Model.
  val model = pipeline.fit(trainingDataset)

  // Predictions.
  val predictions = model.transform(testDataset)
  predictions.createOrReplaceTempView("price_increase_predictions")
  sqlContext.sql("select * from price_increase_predictions order by price desc").show(10)

  // Evaluator.
  val evaluator = new RegressionEvaluator()
    .setLabelCol(priceColumn)
    .setPredictionCol(predictionColumn)
    .setMetricName("mae")

  // Evaluate.
  val trainingCount = trainingDataset.count
  val testCount = testDataset.count
  val trainingTestCountRatio = testCount.toDouble / trainingCount.toDouble
  println(s"1. Training count: $trainingCount")
  println(s"2. Test count: $testCount")
  println(s"3. Training / Test ratio: $trainingTestCountRatio")
  println(s"4. Predictions count: ${predictions.count}")
  println(s"5. Mean Absolute Error: ${evaluator.evaluate(predictions)}")

  sparkListener.log()
  sparkSession.stop()
}