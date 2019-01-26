package spark

import org.scalatest.{FunSuite, Matchers}

class AccumulatorTest extends FunSuite with Matchers {
  import SparkInstance._

  test("accumulator") {
    val accumulator = sparkContext.longAccumulator
    sparkContext.parallelize(seq = 1 to 1000, numSlices = 4).foreach(x => accumulator.add(x.toLong))
    accumulator.value shouldBe 500500
  }
}