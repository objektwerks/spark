name := "spark"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.12.8"
libraryDependencies ++= {
  val sparkVersion = "2.4.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-graphx" % sparkVersion,
    "org.scalikejdbc" %% "scalikejdbc" % "3.3.3",
    "com.h2database" % "h2" % "1.4.197",
    "org.slf4j" % "slf4j-api" % "1.7.25",
    "org.scalatest" %% "scalatest" % "3.0.5" % Test
  )
}