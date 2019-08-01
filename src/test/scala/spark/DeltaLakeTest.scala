package spark

import org.scalatest.{FunSuite, Matchers}

class DeltaLakeTest extends FunSuite with Matchers {
  import SparkInstance._

  test("lake") {
    println(s"Spark User: ${sparkContext.sparkUser}")
  }
}