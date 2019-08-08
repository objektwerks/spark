package spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Encoders, ForeachWriter}

case class Person(id: Long, age: Long, name: String, role: String)

object Person {
  val personSchema = Encoders.product[Person].schema
  val personStructType = new StructType()
    .add("id", IntegerType)
    .add("age", IntegerType)
    .add("name", StringType)
    .add("role", StringType)
  val personForeachWriter = new ForeachWriter[Person] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(person: Person): Unit = println(s"$person")
    override def close(errorOrNull: Throwable): Unit = ()
  }
  implicit def personOrdering: Ordering[Person] = Ordering.by(_.name)
}

case class Task(tid: Long, pid: Long, task: String)

object Task {
  val taskSchema = Encoders.product[Task].schema
  implicit def taskOrdering: Ordering[Task] = Ordering.by(_.task)
}

case class AvgAgeByRole(role: String, avg_age: Double)

object AvgAgeByRole {
  val avgAgeByRoleSchema = Encoders.product[AvgAgeByRole].schema
  implicit def avgAgeByRoleOrdering: Ordering[AvgAgeByRole] = Ordering.by(role => role.avg_age > role.avg_age)
}