package spark

import org.scalatest.{FunSuite, Matchers}

class AccumulatorTest extends FunSuite with Matchers {
  import SparkInstance._

  test("accumulator") {
    val accumulator = sparkContext.longAccumulator(name = "accumulator")
    accumulator.add(1)
    accumulator.value shouldBe 1
  }
}