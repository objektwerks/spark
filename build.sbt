name := "spark"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.20"
libraryDependencies ++= {
  val sparkVersion = "2.4.8"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-graphx" % sparkVersion,
    "io.delta" %% "delta-core" % "2.4.0",
    "org.scalikejdbc" %% "scalikejdbc" % "4.3.2",
    "com.h2database" % "h2" % "2.3.232",
    "org.slf4j" % "slf4j-api" % "2.0.12",
    "org.scalatest" %% "scalatest" % "3.2.19" % Test
  )
}
