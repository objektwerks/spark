package spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import spark.SparkInstance

/*
  id,country,description,designation,points,price,province,region_1,region_2,variety,winery
  id - 0,
  country - US,
  description - "This tremendous 100% varietal wine hails from Oakville and was aged over three years in oak.
   Juicy red-cherry fruit and a compelling hint of caramel greet the palate, framed by elegant,
   fine tannins and a subtle minty tone in the background. Balanced and rewarding from start to
   finish, it has years ahead of it to develop further nuance. Enjoy 2022–2030.",
  designation - Martha's Vineyard,
  points - 96,
  price - 235.0,
  province - California,
  region_1 - Napa Valley,
  region_2 - Napa,
  variety - Cabernet Sauvignon,
  winery - Heitz
 */
object WineRatingApp extends App {
  import SparkInstance._

  // Data. TODO!
  val schemaStruct = StructType(
    StructField("points", DoubleType) ::
      StructField("country", StringType) ::
      StructField("price", DoubleType) :: Nil
  )
  val dataframe = sparkSession.read.format("csv").option("header", "true").schema(schemaStruct).load("./data/wine/*.csv").na.drop()
  val Array(trainingData, testData) = dataframe.randomSplit(Array(0.8, 0.2))
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