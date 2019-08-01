package spark

import org.scalatest.{FunSuite, Matchers}

class DeltaLakeTest extends FunSuite with Matchers {
  import SparkInstance._
  val personsPath = "./target/delta/persons"
  val personsDataframe = sparkSession.read.json("./data/person/person.json")

  test("write -> read") {
    personsDataframe.write.format("delta").mode("overwrite").save(personsPath)
    val personsDelta = sparkSession.read.format("delta").load(personsPath)
    personsDelta.select("*").show
  }
}