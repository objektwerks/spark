package spark

import org.scalatest.{FunSuite, Matchers}

import scala.io.Source

class RatingsTest extends FunSuite with Matchers {
  import SparkInstance._

  test("count") {
    val data = Source.fromInputStream(this.getClass.getResourceAsStream("/ratings/u.data")).getLines.toSeq
    val lines = sparkContext.makeRDD(data)
    val ratings = lines.map(line => line.split("\t")(2).toInt)
    val ratingsByCount = ratings.countByValue
    ratingsByCount(1) shouldBe 6110
    ratingsByCount(2) shouldBe 11370
    ratingsByCount(3) shouldBe 27145
    ratingsByCount(4) shouldBe 34174
    ratingsByCount(5) shouldBe 21201
  }

  test("average") {
    def parseLine(line: String): (Int, Int) = {
      val fields = line.split(",")
      (fields(2).toInt, fields(3).toInt)
    }

    val data = Source.fromInputStream(this.getClass.getResourceAsStream("/friends.csv")).getLines.toSeq
    val lines = sparkContext.makeRDD(data)
    val rdd = lines.map(parseLine)
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // (age, (friends, 1))
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2) // (age, friends)
    val results = averagesByAge.collect.sorted
    (18, 343) shouldEqual results.head
    (69, 235) shouldEqual results.last
  }
}