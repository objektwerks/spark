package spark

import org.apache.log4j.Logger
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

case class Person(age: Long, name: String)

object Person {
  val logger = Logger.getLogger(this.getClass)

  implicit def ordering: Ordering[Person] = Ordering.by(_.age)

  val personStructType = new StructType().add("age", IntegerType).add("name", StringType)

  val personForeachWriter = new ForeachWriter[Person] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(person: Person): Unit = logger.info(s"*** $person")
    override def close(errorOrNull: Throwable): Unit = logger.info("*** Closing person foreach writer...")
  }

  class Average(ages: Array[Long]) {
    def avg: Double = ages.sum / ages.length.toDouble
  }

  implicit def avg(ages: Array[Long]) = new Average(ages)
}