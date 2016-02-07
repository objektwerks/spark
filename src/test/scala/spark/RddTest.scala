package spark

import org.apache.spark.HashPartitioner
import org.scalatest.FunSuite

import scala.collection.SortedSet

class RddTest extends FunSuite {
  val context = SparkInstance.context

  test("transformations with action") {
    val rdd = context.makeRDD(Array(1, 2, 3)).cache
    assert(rdd.filter(_ % 2 == 0).first == 2)
    assert(rdd.filter(_ % 2 != 0).first == 1)
    assert(rdd.map(_ + 1).sum == 9)
    assert(rdd.map(_ + 1).collect sameElements Array(2, 3, 4))
  }

  test("actions") {
    val rdd = context.makeRDD(Array(1, 2, 3)).cache
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
    val rdd = context.parallelize(data)
    val result = rdd.filter(_ % 2 == 0).collect
    assert(result.length == 500000)
  }

  test("partitioner") {
    val rdd = context.parallelize(List((1, 1), (2, 2), (3, 3))).partitionBy(new HashPartitioner(2)).persist
    val partitioner = rdd.partitioner.get // ShuffleRDDPartition @0 / @1
    assert(partitioner.numPartitions == 2)
  }

  test("aggregate") {
    val data = 1 to 10
    val rdd = context.parallelize(data)
    val (x, y) = rdd.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    assert(x == 55 && y == 10)
  }

  test("join") {
    val leftRdd = context.makeRDD(Array((1, 2)))
    val rightRdd = context.makeRDD(Array((1, 3)))
    val joinRdd = leftRdd.join(rightRdd)
    joinRdd.collect foreach { case t:(Int, (Int, Int)) => assert( (1,(2,3)) == t ) }
  }

  test("sets") {
    val rdd1 = context.makeRDD(Array(1, 2, 3)).cache
    val rdd2 = context.makeRDD(Array(3, 4, 5)).cache
    assert(rdd1.union(rdd2).collect sameElements Array(1, 2, 3, 3, 4, 5))
    assert(rdd1.intersection(rdd2).collect sameElements Array(3))
    assert(rdd1.subtract(rdd2).collect.sorted sameElements Array(1, 2))
    assert(rdd2.subtract(rdd1).collect.sorted sameElements Array(4, 5))

    val rdd3 = context.makeRDD(Array(1, 1, 2, 2, 3, 3))
    assert(rdd3.distinct.collect.sorted sameElements Array(1, 2, 3))

    val rdd4 = context.makeRDD(Array(1, 2))
    val rdd5 = context.makeRDD(Array(3, 4))
    assert(rdd4.cartesian(rdd5).collect sameElements Array((1,3), (1, 4), (2, 3), (2, 4)))
  }

  test("reduce by key") {
    val rdd = context.makeRDD(Array((1, 1), (1, 2), (1, 3))).cache
    val (key, aggregate) = rdd.reduceByKey(_ + _).first
    assert(rdd.keys.collect sameElements Array(1, 1, 1))
    assert(rdd.values.collect sameElements Array(1, 2, 3))
    assert(key == 1 && aggregate == 6)
  }

  test("group by key") {
    val rdd = context.makeRDD(Array((1, 1), (1, 2), (1, 3))).cache
    val (key, list) = rdd.groupByKey.first
    assert(rdd.keys.collect sameElements Array(1, 1, 1))
    assert(rdd.values.collect sameElements Array(1, 2, 3))
    assert(key == 1 && list.sum == 6)
  }

  test("sort by key") {
    val rdd = context.makeRDD(Array((3, 1), (2, 2), (1, 3)))
    assert(rdd.reduceByKey(_ + _).sortByKey(ascending = true).collect sameElements Array((1,3), (2, 2), (3, 1)))
  }

  test("map values") {
    val rdd = context.makeRDD(Array((1, 1), (1, 2), (1, 3)))
    val (key, values) = rdd.mapValues(_ * 2).groupByKey.first
    assert(key == 1 && values.sum == 12)
  }

  test("text") {
    val rdd = context.makeRDD(SparkInstance.license).cache
    val totalLines = rdd.count
    assert(totalLines == 19)

    val selectedWordCount = rdd.filter(_.contains("Permission")).count
    assert(selectedWordCount == 1)

    val longestLine = rdd.map(l => l.length).reduce((a, b) => Math.max(a, b))
    assert(longestLine == 77)

    val wordCountRdd = countWords(rdd).cache
    val totalWords = wordCountRdd.map(_._2).sum.toInt
    assert(totalWords == 169)

    val maxCount = wordCountRdd.values.max
    val (word, count) = wordCountRdd.filter(_._2 == maxCount).first
    assert(word == "the" && count == 14)
  }
}