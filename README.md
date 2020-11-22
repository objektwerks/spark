Spark
-----
>Tests Spark 2 features.

Architecture
------------
* Job 1 --> * Stage 1 --> * Partition | Task
* Driver 1 --> * Executor
* JVM 1 --> 1 Executor
* Executor 1 --> * Partition | Task
* Task 1 --> 1 Partition

Test
----
1. sbt clean test

Run
---
1. sbt clean compile run

* [1] objektwerks.DeltaLakeApp
* [2] objektwerks.FlightGraphApp
* [3] objektwerks.KMeansApp
* [4] objektwerks.LinearRegressionApp
* [5] objektwerks.LogEntryApp
* [6] objektwerks.RecommendationApp
* [7] objektwerks.WinePricePredictionApp
 
Logs
----
1. target/test.log
2. target/app.log

Events
------
1. target/local-*

Tuning
------
1. kyro serialization
2. partitions
3. driver and executor memory/cores
4. cache/persist/checkpointing
5. narrow vs wide transformations
6. shuffling (disk/network io)
7. splittable files
8. number of files and size
9. data locality
10. jvm gc
11. spark web/history ui
12. tungsten

JDK
---
>Spark 2 requires JDK 8. Via Sbt, use as follows:

* sbt clean test -java-home /Users/objektwerks/.sdkman/candidates/java/8.0.275.hs-adpt
* sbt run -java-home /Users/objektwerks/.sdkman/candidates/java/8.0.275.hs-adpt
 
.sbtopts
--------
1. Create an .sbtopts file in the project root directory.
2. Add this line: -java-home /Users/objektwerks/.sdkman/candidates/java/8.0.275.hs-adpt
