package spark

import org.scalatest.{FunSuite, Matchers}

class DeltaLakeTest extends FunSuite with Matchers {
  import SparkInstance._

  test("batch") {
    val personsPath = "./target/delta/persons"
    val personsDataframe = sparkSession.read.json("./data/person/person.json")
    personsDataframe.write.format("delta").mode("overwrite").save(personsPath)
    val personsDelta = sparkSession.read.format("delta").load(personsPath)
    personsDelta.select("*").show
    personsDelta.select("*").count shouldBe 4
  }

  test("structured streaming") {
    import Person._
    val rolesPath = "./target/delta/roles"
    sparkSession
      .readStream
      .option("basePath", "./data/person")
      .schema(personStructType)
      .json("./data/person")
      .groupBy("role", "name")
      .count
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "./target/delta/roles/checkpoints")
      .start(rolesPath)
      .awaitTermination(9000L) // Time-dependent due to slow Delta Lake IO!
    sparkSession
      .readStream
      .format("delta")
      .load(rolesPath)
      .writeStream
      .format("console")
      .outputMode("append")
      .start
      .awaitTermination(3000L)
    val rolesDelta = sparkSession.read.format("delta").load(rolesPath)
    rolesDelta.select("*").show
    rolesDelta.select("*").count shouldBe 4
  }
}