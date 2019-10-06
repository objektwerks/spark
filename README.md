Spark
-----
>The purpose of the project is to test Spark features.

Test
----
1. sbt clean test

Run
---
1. sbt clean test run
 
    * [1] spark.deltalake.DeltaLakeApp
    * [2] spark.graphx.FlightDataApp
    * [3] spark.ml.KMeansApp
    * [4] spark.ml.LinearRegressionApp
    * [5] spark.ml.RecommendationApp
    * [6] spark.ml.WinePricePredictionApp
    * [7] spark.streaming.LogEntryApp

Logs
----
1. ./target/test.log
2. ./target/app.log

Events
------
1. ./target/local-*

Tunning
------- 
>kyro serialization, partitions, driver and executor memory/cores, cache/persist/checkpointing, narrow vs wide transformations,
>shuffling (disk/network io), splittable files, number of files and size, data locality, jvm gc, spark web/history ui,
>tungsten

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