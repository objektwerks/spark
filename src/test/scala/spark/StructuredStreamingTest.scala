package spark

import org.scalatest.{FunSuite, Matchers}

class StructuredStreamingTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("structured streaming") {
    import Person._
    val in = sparkSession
      .readStream
      .option("basePath", "./data/json")
      .schema(personStructType)
      .json("./data/json")
      .as[Person]
    val out = in
      .writeStream
      .foreach(personForeachWriter)
    val query = out.start()
    query.awaitTermination(1000L)
  }
}