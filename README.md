Spark
-----
>The purpose of the project is to test Spark features.

Test
----
1. sbt clean test

Run
---
1. sbt clean test run

 [1] spark.graphx.FlightDataApp
 [2] spark.ml.WineRatingApp
 [3] spark.mlib.KMeansApp
 [4] spark.mlib.LinearRegressionApp
 [5] spark.mlib.RecommendationApp
 [6] spark.streaming.LogEntryApp
 
Output
------
1. ./target/test.log
2. ./target/app.log
3. ./target/testdb.mv.db