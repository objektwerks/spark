package spark

import org.scalatest.{FunSuite, Matchers}
import spark.entity.Person

class StructuredStreamingTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("structured streaming") {
    import Person._
    sparkSession
      .readStream
      .option("basePath", "./data/person")
      .schema(personStructType)
      .json("./data/person")
      .as[Person]
      .writeStream
      .foreach(personForeachWriter)
      .start
      .awaitTermination(3000L)
  }
}