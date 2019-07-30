package spark

import org.apache.spark.util.LongAccumulator
import org.scalatest.{FunSuite, Matchers}

class AccumulatorTest extends FunSuite with Matchers {
  import SparkInstance._

  test("accumulator") {
    val accumulator = new LongAccumulator()
    val accumulatorName = "longAccumulator"
    sparkContext.register(accumulator, accumulatorName)
    accumulator.add(1)
    accumulator.value shouldBe 1
  }
}