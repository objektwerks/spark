package spark.entity

import org.apache.spark.sql.Encoders

case class AvgAgeByRole(role: String, avg_age: Double)

object AvgAgeByRole {
  val avgAgeByRoleSchema = Encoders.product[AvgAgeByRole].schema
  implicit def avgAgeByRoleOrdering: Ordering[AvgAgeByRole] = Ordering.by(role => role.avg_age > role.avg_age)
}