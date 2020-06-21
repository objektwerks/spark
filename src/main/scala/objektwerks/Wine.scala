package objektwerks

import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Encoders, ForeachWriter}

case class Wine(id: Int,
                country: String,
                description: String,
                designation: String,
                points: Int,
                price: Double,
                province: String,
                region_1: String,
                region_2: String,
                variety: String,
                winery: String)

object Wine {
  val logger = Logger.getLogger(this.getClass)
  val wineSchema = Encoders.product[Wine].schema
  val wineStructType = new StructType()
    .add("id", IntegerType)
    .add("country", StringType)
    .add("description", StringType)
    .add("points", IntegerType)
    .add("price", DoubleType)
    .add("province", StringType)
    .add("region_1", StringType)
    .add("region_2", StringType)
    .add("variety", StringType)
    .add("winery", StringType)
  val wineForeachWriter = new ForeachWriter[Wine] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(wine: Wine): Unit = logger.info(s"*** $wine")
    override def close(errorOrNull: Throwable): Unit = logger.info("*** Closing wine foreach writer...")
  }

  implicit def wineOrdering: Ordering[Wine] = Ordering.by(_.variety)
}