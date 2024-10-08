Spark
-----
>Spark 2 apps and tests.

Warning
-------
>This project is fundamentally broken due to library ***version*** and ***classloader*** issues.

>See [Spark3](https://github.com/objektwerks/spark3), instead!

Architecture
------------
>This model excludes the cluster manager, such as Standalone, Yarn, Mesos and Kubernetes.
* Job 1 --> * Stage 1 --> * Task
* Driver 1 <--> * Executor
* Node 1 --> * JVM 1 --> * Executor
* Executor 1 --> * Task | Partition
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
