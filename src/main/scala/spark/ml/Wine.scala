package spark.ml

import org.apache.spark.sql.Encoders

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
  val wineSchema = Encoders.product[Wine].schema
  implicit def wineOrdering: Ordering[Wine] = Ordering.by(_.variety)
}