package spark.entity

import org.apache.spark.sql.Encoders

case class PersonsTasks(id: Long, age: Long, name: String, role: String, tid: Long, pid: Long, task: String)

object PersonsTasks {
  val personsTasksSchema = Encoders.product[PersonsTasks].schema
}