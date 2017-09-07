package spark

import org.apache.log4j.Logger
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

case class Person(age: Long, name: String)

object Person {
  val logger = Logger.getLogger(this.getClass)

  val personStructType = new StructType().add("age", IntegerType).add("name", StringType)
  val personForeachWriter = new ForeachWriter[Person] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(person: Person): Unit = logger.info(s"*** $person")
    override def close(errorOrNull: Throwable): Unit = logger.info("*** Closing person foreach writer...")
  }
}

case class Age(number: Long = 0) extends AnyVal {
  implicit def +(other: Age): Age = Age(number + other.number)
}

object Age {
  class Average(ages: Array[Age]) {
    def avg: Age = Age( ages.reduce( ( a, b ) => a + b ).number / ages.length )
  }
  class Total(ages: Array[Age]) {
    def total: Age = ages.reduce(_ + _)
  }
  implicit def ageOrdering: Ordering[Age] = Ordering.by(_.number)
  implicit def average(ages: Array[Age]) = new Average(ages)
  implicit def total(ages: Array[Age]) = new Total(ages)
}