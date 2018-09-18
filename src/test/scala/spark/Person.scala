package spark

import org.apache.log4j.Logger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Encoders, ForeachWriter}

case class Person(age: Long, name: String, role: String)

object Person {
  val logger = Logger.getLogger(this.getClass)
  val personSchema = Encoders.product[Person].schema
  val personStructType = new StructType()
    .add("age", IntegerType)
    .add("name", StringType)
    .add("role", StringType)
  val personForeachWriter = new ForeachWriter[Person] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(person: Person): Unit = logger.info(s"*** $person")
    override def close(errorOrNull: Throwable): Unit = logger.info("*** Closing person foreach writer...")
  }

  implicit def personOrdering: Ordering[Person] = Ordering.by(_.name)

  implicit class Average(ages: Array[Long]) {
    def avg: Double = ages.sum / ages.length.toDouble
  }
}