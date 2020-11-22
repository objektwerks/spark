Spark2
------
>This project tests Spark 2 features.

WARNING
-------
>JDK 8 must be used with Spark 2. See .sbtopts

Test
----
1. sbt clean test

Bloop
-----
1. sbt bloopInstall
2. bloop projects
3. bloop clean spark
4. bloop compile spark
5. bloop test spark

Run
---
1. sbt clean test run

    * [1] objektwerks.DeltaLakeApp
    * [2] objektwerks.FlightGraphApp
    * [3] objektwerks.KMeansApp
    * [4] objektwerks.LinearRegressionApp
    * [5] objektwerks.LogEntryApp
    * [6] objektwerks.RecommendationApp
    * [7] objektwerks.WinePricePredictionApp
 
Logs
----
1. ./target/test.log
2. ./target/app.log

Events
------
1. ./target/local-*

Tunning
------- 
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

JDKs
----
>If you have more than one JDK installed, such as JDK 8 and JDK 11, you need to run sbt using JDK 8.
Here's a few examples:

* sbt clean test -java-home /Users/objektwerks/.sdkman/candidates/java/8.0.275.hs-adpt
* sbt run -java-home /Users/objektwerks/.sdkman/candidates/java/8.0.275.hs-adpt

>Or, optionally, create an .sbtopts file.
 
.sbtopts
--------
1. Create an .sbtopts file in the project root directory.
2. Add this line ( to line 1 ): -java-home /Users/objektwerks/.sdkman/candidates/java/8.0.275.hs-adpt
