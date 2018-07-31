package spark

import org.scalatest.{FunSuite, Matchers}

class StructuredStreamingTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("structured streaming") {
    import Person._
    val persons = sparkSession
      .readStream
      .option("basePath", "./data/person")
      .schema(personStructType)
      .json("./data/person")
      .as[Person]
    val query = persons
      .writeStream
      .foreach(personForeachWriter)
      .start()
    query.awaitTermination(1000L)
  }
}