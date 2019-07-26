Spark
-----
>The purpose of the project is to test Spark features.

TODO
----
1. Enhance Dataset and Dataframe expressions

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
 
Logs
----
1. ./target/test.log
2. ./target/app.log

JDKs
----
>If you have more than one JDK installed, such as JDK 8 and JDK 11, you need to run sbt using JDK 8.
Here's a few examples:

* sbt clean test -java-home /Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home
* sbt run -java-home /Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home

>Or, optionally, create an .sbtopts file.
 
.sbtopts
--------
1. Create an .sbtopts file in the project root directory.
2. Add this line ( to line 1 ): -java-home /Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home