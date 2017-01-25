name := "spark"
organization := "objektwerks"
version := "0.1"
scalaVersion := "2.11.8"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
libraryDependencies ++= {
  val sparkVersion = "2.1.0"
  Seq(
    "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-streaming_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
    "org.apache.spark" % "spark-mllib_2.11" % sparkVersion % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.21" % "test",
    "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
  )
}
scalacOptions ++= Seq(
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-feature",
  "-Ywarn-unused-import",
  "-Ywarn-unused",
  "-Ywarn-dead-code",
  "-unchecked",
  "-deprecation",
  "-Xfatal-warnings",
  "-Xlint:missing-interpolator",
  "-Xlint"
)
javaOptions += "-server -Xss1m -Xmx2g"
fork in test := true
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

import java.util.jar.Attributes.Name._
packageOptions in (Compile, packageBin) += {
  Package.ManifestAttributes(MAIN_CLASS -> "spark.SparkAppLauncher")
}
exportJars := true
artifactName := { (s: ScalaVersion, m: ModuleID, a: Artifact) => "spark-app-0.1.jar" }

assemblyMergeStrategy in assembly := {
  case "license.mit" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
test in assembly := {}
mainClass in assembly := Some("spark.SparkAppLauncher")
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "spark-app-0.1.jar"