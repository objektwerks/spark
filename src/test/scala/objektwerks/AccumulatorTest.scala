package objektwerks

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class AccumulatorTest extends AnyFunSuite with Matchers {
  import SparkInstance._

  test("accumulator") {
    val longAcc = sparkContext.longAccumulator
    longAcc.add(1)
    longAcc.value shouldBe 1

    val doubleAcc = sparkContext.doubleAccumulator
    doubleAcc.add(1.0)
    doubleAcc.value shouldBe 1.0

    val collectionAcc = sparkContext.collectionAccumulator[Int]
    collectionAcc.add(1)
    collectionAcc.add(2)
    collectionAcc.add(3)
    collectionAcc.value.asScala.sum shouldEqual 6
  }
}