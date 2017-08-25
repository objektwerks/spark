package spark

import java.nio.charset.CodingErrorAction

import breeze.linalg.{max, min}
import org.apache.spark.HashPartitioner
import org.scalatest.{FunSuite, Matchers}

import scala.collection.{SortedSet, mutable}
import scala.io.{Codec, Source}

class RddTest extends FunSuite with Matchers {
  import SparkInstance._

  test("transformations with action") {
    val rdd = sparkContext.makeRDD(Array(1, 2, 3)).cache
    assert(rdd.filter(_ % 2 == 0).first == 2)
    assert(rdd.filter(_ % 2 != 0).first == 1)
    assert(rdd.map(_ + 1).sum == 9)
    assert(rdd.map(_ + 1).collect sameElements Array(2, 3, 4))
  }

  test("actions") {
    val rdd = sparkContext.makeRDD(Array(1, 2, 3)).cache
    assert(rdd.count == 3)
    assert(rdd.first == 1)
    assert(rdd.min == 1)
    assert(rdd.max == 3)
    assert(rdd.mean == 2.0)
    assert(rdd.variance == 0.6666666666666666)
    assert(rdd.sampleVariance == 1.0)
    assert(rdd.stdev == 0.816496580927726)
    assert(rdd.sampleStdev == 1.0)
    assert(rdd.sum == 6)
    assert(rdd.fold(0)(_ + _) == 6)
    assert(rdd.reduce(_ + _) == 6)
    assert(rdd.takeOrdered(3).toSet == SortedSet(1, 2, 3))
    assert(rdd.take(1) sameElements Array(1))
  }

  test("parallelize") {
    val data = 1 to 1000000
    val rdd = sparkContext.parallelize(data)
    val result = rdd.filter(_ % 2 == 0).collect
    assert(result.length == 500000)
  }

  test("partitioner") {
    val rdd = sparkContext.parallelize(List((1, 1), (2, 2), (3, 3))).partitionBy(new HashPartitioner(2)).persist
    val partitioner = rdd.partitioner.get // ShuffleRDDPartition @0 / @1
    assert(partitioner.numPartitions == 2)
  }

  test("aggregate") {
    val data = 1 to 10
    val rdd = sparkContext.parallelize(data)
    val (x, y) = rdd.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    assert(x == 55 && y == 10)
  }

  test("cogroup") {
    val leftRdd = sparkContext.makeRDD(Array((1, 2)))
    val rightRdd = sparkContext.makeRDD(Array((1, 3)))
    val cogroupRdd = leftRdd.cogroup(rightRdd)
    cogroupRdd foreach println
    cogroupRdd.collect foreach { t:(Int, (Iterable[Int], Iterable[Int])) => assert( (1,(Iterable(2),Iterable(3))) == t ) }
  }

  test("join") {
    val leftRdd = sparkContext.makeRDD(Array((1, 2)))
    val rightRdd = sparkContext.makeRDD(Array((1, 3)))
    val joinRdd = leftRdd.join(rightRdd)
    joinRdd.collect foreach { t:(Int, (Int, Int)) => assert( (1, (2, 3)) == t ) }
  }

  test("sets") {
    val rdd1 = sparkContext.makeRDD(Array(1, 2, 3)).cache
    val rdd2 = sparkContext.makeRDD(Array(3, 4, 5)).cache
    assert(rdd1.union(rdd2).collect sameElements Array(1, 2, 3, 3, 4, 5))
    assert(rdd1.intersection(rdd2).collect sameElements Array(3))
    assert(rdd1.subtract(rdd2).collect.sorted sameElements Array(1, 2))
    assert(rdd2.subtract(rdd1).collect.sorted sameElements Array(4, 5))

    val rdd3 = sparkContext.makeRDD(Array(1, 1, 2, 2, 3, 3))
    assert(rdd3.distinct.collect.sorted sameElements Array(1, 2, 3))

    val rdd4 = sparkContext.makeRDD(Array(1, 2))
    val rdd5 = sparkContext.makeRDD(Array(3, 4))
    assert(rdd4.cartesian(rdd5).collect sameElements Array((1,3), (1, 4), (2, 3), (2, 4)))
  }

  test("reduce by key") {
    val rdd = sparkContext.makeRDD(Array((1, 1), (1, 2), (1, 3))).cache
    val (key, aggregate) = rdd.reduceByKey(_ + _).first
    assert(rdd.keys.collect sameElements Array(1, 1, 1))
    assert(rdd.values.collect sameElements Array(1, 2, 3))
    assert(key == 1 && aggregate == 6)
  }

  test("group by key") {
    val rdd = sparkContext.makeRDD(Array((1, 1), (1, 2), (1, 3))).cache
    val (key, list) = rdd.groupByKey.first
    assert(rdd.keys.collect sameElements Array(1, 1, 1))
    assert(rdd.values.collect sameElements Array(1, 2, 3))
    assert(key == 1 && list.sum == 6)
  }

  test("sort by key") {
    val rdd = sparkContext.makeRDD(Array((3, 1), (2, 2), (1, 3)))
    assert(rdd.reduceByKey(_ + _).sortByKey(ascending = true).collect sameElements Array((1,3), (2, 2), (3, 1)))
  }

  test("map values") {
    val rdd = sparkContext.makeRDD(Array((1, 1), (1, 2), (1, 3)))
    assert(12 == rdd.mapValues(_ * 2).values.sum)
  }

  test("count") {
    val rdd = sparkContext.makeRDD(SparkInstance.licenseText).cache
    val totalLines = rdd.count
    assert(totalLines == 19)

    val selectedWordCount = rdd.filter(_.contains("Permission")).count
    assert(selectedWordCount == 1)

    val longestLine = rdd.map(l => l.length).reduce((a, b) => Math.max(a, b))
    assert(longestLine == 77)

    val wordCountRdd = countWords(rdd).cache
    val totalWords = wordCountRdd.map(_._2).sum.toInt
    assert(totalWords == 169)

    val maxWordCount = wordCountRdd.values.max
    val (word, count) = wordCountRdd.filter(_._2 == maxWordCount).first
    assert(word == "the" && count == 14)
  }

  test("ratings ~ count by value") {
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

  test("friends ~ average") {
    def parseLine(line: String): (Int, Int) = {
      val fields = line.split(",")
      val age = fields(2).toInt
      val friends = fields(3).toInt
      (age, friends)
    }

    val data = Source.fromInputStream(this.getClass.getResourceAsStream("/friends.txt")).getLines.toSeq
    val lines = sparkContext.makeRDD(data)
    val parsedLines = lines.map(parseLine)
    val totalsByAge = parsedLines.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) // (age, (friends, 1))
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2) // (age, friends)
    val results = averagesByAge.collect.sorted
    (18, 343) shouldBe results.head
    (69, 235) shouldBe results.last
  }

  test("weather ~ min, max") {
    def parseLine(line: String): (String, String, Float) = {
      val fields = line.split(",")
      val station = fields(0)
      val entry = fields(2)
      val temp = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
      (station, entry, temp)
    }

    val data = Source.fromInputStream(this.getClass.getResourceAsStream("/weather.txt")).getLines.toSeq
    val lines = sparkContext.makeRDD(data)
    val parsedLines = lines.map(parseLine)

    val minTemps = parsedLines.filter(x => x._2 == "TMIN")
    val minStationTemps = minTemps.map(x => (x._1, x._3))
    val minTempsByStation = minStationTemps.reduceByKey((x, y) => min(x, y))
    val minResults = minTempsByStation.collect.sorted
    ("EZE00100082", 7.700001F) shouldBe minResults.head

    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    val maxStationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    val maxTempsByStation = maxStationTemps.reduceByKey( (x,y) => max(x,y))
    val maxResults = maxTempsByStation.collect.sorted
    ("EZE00100082", 90.14F) shouldBe maxResults.head
  }

  test("orders ~ sorted by key") {
    def parseLine(line: String): (Int, Float) = {
      val fields = line.split(",")
      val customer = fields(0).toInt
      val amount = fields(2).toFloat
      (customer, amount)
    }

    val data = Source.fromInputStream(this.getClass.getResourceAsStream("/orders.txt")).getLines.toSeq
    val lines = sparkContext.makeRDD(data)
    val parsedLines = lines.map(parseLine)
    val customerByTotal = parsedLines.reduceByKey((x,y) => x + y)
    val totalByCustomer = customerByTotal.map(kv => (kv._2, kv._1))
    val totalByCustomerSorted = totalByCustomer.sortByKey()
    val results = totalByCustomerSorted.collect()
    (3309.3804F, 45) shouldBe results.head // min
    (6375.45F, 68) shouldBe results.last // max
    val amounts = results.map(kv => kv._1)
    500489.16F shouldBe amounts.sum
    3309.3804F shouldBe amounts.min
    6375.45F shouldBe amounts.max
    5004.8916F shouldBe amounts.sum / amounts.length // avg
  }

  test("ratings ~ broadcast, reduce by key") {
    def loadMovies(): Map[Int, String] = {
      implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

      val moviesById = mutable.Map[Int, String]()
      val lines = Source.fromInputStream(this.getClass.getResourceAsStream("/ratings/u.item")).getLines
      for (line <- lines) {
        val fields = line.split('|')
        if (fields.length > 1) moviesById += (fields(0).toInt -> fields(1))
      }
      moviesById.toMap
    }

    val broadcastMovies = sparkContext.broadcast(loadMovies())
    val data = Source.fromInputStream(this.getClass.getResourceAsStream("/ratings/u.data")).getLines.toSeq
    val lines = sparkContext.makeRDD(data)
    val movies = lines.map( line => ( line.split("\t")(1).toInt, 1 ) )
    val movieCounts = movies.reduceByKey( (x, y) => x + y )
    val countMovies = movieCounts.map( movieCount => (movieCount._2, movieCount._1) )
    val sortedCountMovies = countMovies.sortByKey()
    val sortedMovieNamesByCount = sortedCountMovies.map( countMovie  => (broadcastMovies.value(countMovie._2), countMovie._1) )
    val results = sortedMovieNamesByCount.collect()
    ("Mostro, Il (1994)", 1) shouldBe results.head  // least popular
    ("Star Wars (1977)", 583) shouldBe results.last // most popular
  }

  test("marvel ~ analysis") {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    def countCoAppearances(line: String) = {
      val elements = line.split("\\s+")
      ( elements(0).toInt, elements.length - 1 )
    }

    def parseNames(line: String) : Option[(Int, String)] = {
      val fields = line.split('\"')
      if (fields.length > 1) Some( (fields(0).trim().toInt, fields(1)) ) else None
    }

    val marvelNameData = Source.fromInputStream(this.getClass.getResourceAsStream("/marvel-names.txt")).getLines.toSeq
    val marvelNameLines = sparkContext.makeRDD(marvelNameData)
    val marvelNames = marvelNameLines.flatMap(parseNames)

    val marvelGraphData = Source.fromInputStream(this.getClass.getResourceAsStream("/marvel-graph.txt")).getLines.toSeq
    val marvelGraphLines = sparkContext.makeRDD(marvelGraphData)
    val marvelGraph = marvelGraphLines.map(countCoAppearances)

    val heroByCoAppearances = marvelGraph.reduceByKey( (x, y) => x + y )
    val coAppearancesByHero = heroByCoAppearances.map( x => (x._2, x._1) )
    val maxCoAppearancesByHero = coAppearancesByHero.max
    val heroWithMostCoAppearances = marvelNames.lookup(maxCoAppearancesByHero._2).head
    heroWithMostCoAppearances shouldBe "CAPTAIN AMERICA"
    maxCoAppearancesByHero._1 shouldBe 1933
  }
}