package spark

import org.scalatest.{FunSuite, Matchers}

class BroadcastTest extends FunSuite with Matchers {
  import SparkInstance._

  test("broadcast") {
    val map = Map("fred" -> "wilma", "barney" -> "betty")
    val broadcastMap = sparkContext.broadcast(map)
    val wives = sparkContext.parallelize(seq = Seq("fred", "barney")).map(broadcastMap.value).collect
    wives(0) shouldBe "wilma"
    wives(1) shouldBe "betty"
  }
}