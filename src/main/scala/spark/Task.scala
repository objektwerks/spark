package spark

import org.apache.spark.sql.Encoders

case class Task(tid: Long, pid: Long, task: String)

object Task {
  val taskSchema = Encoders.product[Task].schema
  implicit def taskOrdering: Ordering[Task] = Ordering.by(_.task)
}