package spark

import java.nio.charset.CodingErrorAction

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.{Codec, Source}

object RecommendationApp extends App {
  val sparkSession = SparkSession.builder.master("local[2]").appName("recommendation").getOrCreate()
  val sparkContext = sparkSession.sparkContext

  val movieIdToNameMap = loadMovieIdToNameMap("/movies.txt")
  val movieRatings = loadMovieRatings("/movie.ratings.txt")

  val rank = 8
  val numberOfIterations = 20
  val model = ALS.train(movieRatings, rank, numberOfIterations)

  val userID = 1
  val userMovieRatings = movieRatings.filter(rating => rating.user == userID)
  val userMovieRatingsAsArray = userMovieRatings.collect()
  println("Ratings:")
  userMovieRatingsAsArray.foreach { rating =>
    println(movieIdToNameMap(rating.product.toInt) + ": " + rating.rating.toString)
  }

  val recommendations = model.recommendProducts(userID, 10)
  println("Recommendations:")
  recommendations.foreach {
    recommendation => println(movieIdToNameMap(recommendation.product.toInt) + " score " + recommendation.rating)
  }

  sparkSession.stop()

  def loadMovieIdToNameMap(moviesTextFilePath: String): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val moviesById = mutable.Map[Int, String]()
    val lines = Source.fromInputStream(this.getClass.getResourceAsStream(moviesTextFilePath)).getLines
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) moviesById += (fields(0).toInt -> fields(1))
    }
    moviesById.toMap
  }

  def loadMovieRatings(movieRatingsTextFilePath: String): RDD[Rating] = {
    val lines = Source.fromInputStream(this.getClass.getResourceAsStream(movieRatingsTextFilePath)).getLines.toSeq
    val rdd = sparkContext.makeRDD(lines)
    rdd.map(line => line.split('\t') ).map(lines => Rating(lines(0).toInt, lines(1).toInt, lines(2).toDouble)).cache()
  }
}