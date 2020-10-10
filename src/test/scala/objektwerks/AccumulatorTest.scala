package objektwerks

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class AccumulatorTest extends AnyFunSuite with Matchers {
  import SparkInstance._

  test("accumulator") {
    val longAcc = sparkContext.longAccumulator("longAcc")
    longAcc.add(1)
    longAcc.name.get shouldBe "longAcc"
    longAcc.value shouldBe 1

    val doubleAcc = sparkContext.doubleAccumulator("doubleAcc")
    doubleAcc.add(1.0)
    doubleAcc.name.get shouldBe "doubleAcc"
    doubleAcc.value shouldBe 1.0

    val intsAcc = sparkContext.collectionAccumulator[Int]("intsAcc")
    intsAcc.add(1)
    intsAcc.add(2)
    intsAcc.add(3)
    intsAcc.name.get shouldBe "intsAcc"
    intsAcc.value.asScala.sum shouldEqual 6
  }
}