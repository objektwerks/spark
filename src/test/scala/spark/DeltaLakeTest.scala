package spark

import org.scalatest.{FunSuite, Matchers}

class DeltaLakeTest extends FunSuite with Matchers {
  import SparkInstance._

  test("write -> read") {
    val personsPath = "./target/delta/persons"
    val personsDataframe = sparkSession.read.json("./data/person/person.json")
    personsDataframe.write.format("delta").mode("overwrite").save(personsPath)
    val personsDelta = sparkSession.read.format("delta").load(personsPath)
    personsDelta.select("*").show
  }
}