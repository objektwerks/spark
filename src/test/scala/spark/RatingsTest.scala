package spark

import org.scalatest.{FunSuite, Matchers}

import scala.io.Source

class RatingsTest extends FunSuite with Matchers {
  import SparkInstance._

  test("count") {
    // u.data = ( userid, movieid, rating, timestamp )
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
}