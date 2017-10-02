package spark

import org.scalatest.{FunSuite, Matchers}

class AccumulatorTest extends FunSuite with Matchers {
  import SparkInstance._

  test("accumulator") {
    val accumulator = sparkContext.longAccumulator
    sparkContext.parallelize(1 to 1000, 4).foreach(x => accumulator.add(x))
    accumulator.value shouldBe 500500
  }
}