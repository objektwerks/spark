package objektwerks.entity

import org.apache.spark.sql.Encoders

case class Age(value: Long)

object Age {
  val ageSchema = Encoders.product[Age].schema
}