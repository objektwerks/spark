package objektwerks

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.util.hashing.MurmurHash3

object FlightGraphApp extends App {
  import SparkInstance._
  import sparkSession.implicits._

  val flights = sparkSession
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load("./data/flights/1987.a.csv")
  flights.printSchema()
  val flightsFromTo = flights.select("Origin", "Dest").rdd
  val airportCodes = flights.select($"Origin", $"Dest").flatMap(originDest => Iterable( originDest(0).toString, originDest(1).toString ) ).rdd

  val airportVertices: RDD[(VertexId, String)] = airportCodes.distinct.map(airportCode => ( MurmurHash3.stringHash(airportCode).toLong, airportCode) )
  val flightEdges = flightsFromTo
    .map(originDest => ( (MurmurHash3.stringHash( originDest(0).toString ), MurmurHash3.stringHash( originDest(1).toString ) ), 1))
    .reduceByKey(_ + _)
    .map(airportIds => Edge(airportIds._1._1.asInstanceOf[Long], airportIds._1._2.asInstanceOf[Long], airportIds._2))

  val graph = Graph(airportVertices, flightEdges, "default-airport").persist()
  println(s"Graph vertices: ${graph.numVertices}")
  println(s"Graph edges: ${graph.numEdges}")

  println("Top 10 most frequent flights:")
  graph
    .triplets
    .sortBy(_.attr, ascending = false)
    .map(triplet => s"${triplet.attr} flights from ${triplet.srcAttr} to ${triplet.dstAttr}")
    .take(10)
    .foreach(println)

  println("Top 10 least frequent flights:")
  graph
    .triplets
    .sortBy(_.attr)
    .map(triplet => s"${triplet.attr} flights from ${triplet.srcAttr} to ${triplet.dstAttr}")
    .take(10)
    .foreach(println)

  graph
    .inDegrees
    .join(airportVertices)
    .sortBy(_._2._1, ascending = true)
    .take(1)
    .foreach(degrees => println(s"Most unique incoming flights (flights, airport): ${degrees._2}"))

  graph
    .outDegrees
    .join(airportVertices)
    .sortBy(_._2._1, ascending = false)
    .take(1)
    .foreach(degrees => println(s"Most unique outgoing flights (flights, airport): ${degrees._2}"))

  val ranks = graph
    .pageRank(0.0001)
    .vertices
  println("Top 10 ranked airports:")
  ranks
    .join(airportVertices)
    .sortBy(_._2._1, ascending = false)
    .map(_._2._2)
    .take(10)
    .foreach(println)
}