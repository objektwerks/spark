name := "spark"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.13"
libraryDependencies ++= {
  val sparkVersion = "2.4.7"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-graphx" % sparkVersion,
    "io.delta" %% "delta-core" % "0.6.1",
    // delta-core 0.7.0 Cause NoClassDefFoundError errors on org/apache/spark/sql/connector/catalog/TableProvider
    "org.scalikejdbc" %% "scalikejdbc" % "3.5.0",
    "com.h2database" % "h2" % "1.4.200",
    "org.slf4j" % "slf4j-api" % "1.7.30",
    "org.scalatest" %% "scalatest" % "3.2.7" % Test
  )
}
