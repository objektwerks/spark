package spark

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import spark.entity.Person

class StructuredStreamingTest extends AnyFunSuite with Matchers {
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