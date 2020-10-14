package objektwerks

import org.apache.spark.sql.{Encoders, ForeachWriter}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

final case class Age(value: Long)
object Age {
  val ageSchema = Encoders.product[Age].schema
}

final case class AvgAgeByRole(role: String, avgAge: Double)
object AvgAgeByRole {
  val avgAgeByRoleSchema = Encoders.product[AvgAgeByRole].schema
  implicit def avgAgeByRoleOrdering: Ordering[AvgAgeByRole] = Ordering.by(role => role.avgAge > role.avgAge)
}

final case class Count(value: String, count: Long)
object Count {
  implicit val countSchema = Encoders.product[Count].schema
}

final case class Friend(id: Int, name: String, age: Int, score: Int)

final case class KeyValue(key: Int, value: Int)
object KeyValue {
  val keyValueSchema = Encoders.product[KeyValue].schema
  implicit def keyValueOrdering: Ordering[KeyValue] = Ordering.by(_.key)
}

final case class Person(id: Long, age: Long, name: String, role: String)
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

final case class PersonsTasks(id: Long, age: Long, name: String, role: String, tid: Long, pid: Long, task: String)
object PersonsTasks {
  val personsTasksSchema = Encoders.product[PersonsTasks].schema
}

final case class Task(tid: Long, pid: Long, task: String)
object Task {
  val taskSchema = Encoders.product[Task].schema
  implicit def taskOrdering: Ordering[Task] = Ordering.by(_.task)
}

final case class PersonAsDog(id: Long, age: Long, name: String, role: String, dogAge: Long)
object PersonAsDog {
  val personAsDogSchema = Encoders.product[PersonAsDog].schema
}