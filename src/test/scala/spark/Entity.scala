package spark

import org.apache.log4j.Logger
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

sealed trait Entity

case class Person(age: Long, name: String) extends Entity

object Person {
  val logger = Logger.getLogger(this.getClass)

  val personStructType = new StructType().add("age", IntegerType).add("name", StringType)
  val personForeachWriter = new ForeachWriter[Person] {
    override def open(partitionId: Long, version: Long): Boolean = true
    override def process(person: Person): Unit = logger.info(s"*** $person")
    override def close(errorOrNull: Throwable): Unit = logger.info("*** Closing person foreach writer...")
  }
}

case class Age(count: Long = 0) extends AnyVal {
  implicit def +(other: Age): Age = Age(count + other.count)
}

object Age {
  implicit def ageOrdering: Ordering[Age] = Ordering.by(_.count)
  def avg(ages: Array[Age]): Age = Age( ages.reduce( ( a, b ) => a + b ).count / ages.length )
}