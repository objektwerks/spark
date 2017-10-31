package spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import spark.SparkInstance

/**
  * Source: See https://blog.scalac.io/scala-spark-ml.html for details on this app's foundation.
  * Data: https://www.kaggle.com/zynicide/wine-reviews Kaggle account required.
  *
  * Features: points, variety, province, region
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
  val featuresColumn = "points_variety_province_region"
  val predictionColumn = "predicted_price"

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
  val predictions = model.transform(testDataset).cache
  predictions.createOrReplaceTempView("price_predictions")
  sqlContext.sql("select points, price, region_1, variety, predicted_price from price_predictions order by price desc")
      .coalesce(1).write.option("header", "true").mode("append").csv("./target/wine.price.predictions")
  sqlContext.sql("select * from price_predictions order by price desc").show(10)
  predictions.describe("predicted_price").show

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
  println(f"3. Training / Test ratio: $trainingTestCountRatio%1.2f")
  println(s"4. Predictions count: ${predictions.count}")
  println(f"5. Mean Absolute Error: ${evaluator.evaluate(predictions)}%1.2f\n")

  sparkSession.stop()
}