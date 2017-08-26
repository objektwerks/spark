package spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.io.Source

object SparkInstance {
  val logger = Logger.getLogger(this.getClass)

  val sparkSession = SparkSession.builder
    .master("local[2]")
    .appName("sparky")
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val personJson = Source.fromInputStream(getClass.getResourceAsStream("/person.json.txt")).getLines.toSeq
  val licenseText = Source.fromInputStream(getClass.getResourceAsStream("/license.mit")).getLines.toSeq
  logger.info("*** Initialized spark instance.")

  sys.addShutdownHook {
    sparkSession.stop()
    logger.info("*** Terminated spark instance.")
  }
}