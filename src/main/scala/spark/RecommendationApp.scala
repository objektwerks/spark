package spark

import java.nio.charset.CodingErrorAction

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.{Codec, Source}

object RecommendationApp extends App {
  val sparkSession = SparkSession.builder.master("local[*]").appName("recommendation").getOrCreate()
  val movieIdToNameMap = loadMovieIdToNameMap("./data/txt/movie-data.txt")
  val movieRatings = loadMovieRatings("./data/txt/movie-ratings.txt")

  val rank = 8
  val iterations = 20
  val model = ALS.train(movieRatings, rank, iterations)

  val userId = 1
  val userMovieRatings = movieRatings.filter(rating => rating.user == userId)
  val userMovieRatingsAsArray = userMovieRatings.collect()
  println(s"\nRatings for User [$userId]:\n")
  for (i <- userMovieRatingsAsArray.indices) {
    val rating = userMovieRatingsAsArray(i)
    println(s"${i + 1}. ${movieIdToNameMap(rating.product.toInt)}: ${rating.rating}")
  }

  val userMovieRecommendationsAsArray = model.recommendProducts(userId, 10)
  println(s"\nRecommendations for User [$userId]:\n")
  for (i <- userMovieRecommendationsAsArray.indices) {
    val recommendation = userMovieRecommendationsAsArray(i)
    println(s"${i + 1}. ${movieIdToNameMap(recommendation.product.toInt)} score ${recommendation.rating}")
  }

  sparkSession.stop()

  def loadMovieIdToNameMap(filePath: String): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val moviesById = mutable.Map[Int, String]()
    val lines = Source.fromFile(filePath).getLines()
    lines foreach { line =>
      val fields = line.split('|')
      if (fields.length > 1) moviesById += (fields(0).toInt -> fields(1))
    }
    moviesById.toMap[Int, String]
  }

  def loadMovieRatings(filePath: String): RDD[Rating] = {
    val lines = sparkSession.sparkContext.textFile(filePath)
    lines.map(line => line.split('\t')).map(lines => Rating(lines(0).toInt, lines(1).toInt, lines(2).toDouble)).cache
  }
}