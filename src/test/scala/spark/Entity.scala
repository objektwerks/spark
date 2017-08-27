package spark

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

sealed trait Entity

case class Person(age: Long, name: String) extends Entity

object Person {
  val personStructType = new StructType().add("age", IntegerType).add("name", StringType)
  val personForeachWriter = new ForeachWriter[Person] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(person: Person): Unit = println(person)
    override def close(errorOrNull: Throwable): Unit = println("Closing person foreach writer...")
  }
}