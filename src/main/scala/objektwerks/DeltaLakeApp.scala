package objektwerks

object DeltaLakeApp extends App {
  import SparkInstance._
  import Person.personStructType
  
  batch()
  structuredStreaming()

  def batch(): Unit = {
    val personsPath = "./target/delta/persons"
    val personsDataframe = sparkSession.read.json("./data/person/person.json")
    personsDataframe.write.format("delta").mode("overwrite").save(personsPath)
    val personsDelta = sparkSession.read.format("delta").load(personsPath)
    personsDelta.select("*").show
    assert( personsDelta.select("*").count == 4 )
  }

  def structuredStreaming(): Unit = {
    val rolesPath = "./target/delta/roles"
    sparkSession
      .readStream
      .schema(personStructType)
      .json("./data/person")
      .groupBy("role", "name")
      .count
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "./target/delta/roles/checkpoints")
      .start(rolesPath)
      .awaitTermination(30000) // Time-dependent due to slow Delta Lake IO!
    sparkSession
      .readStream
      .format("delta")
      .load(rolesPath)
      .writeStream
      .format("console")
      .outputMode("append")
      .start
      .awaitTermination(30000)
    val rolesDelta = sparkSession.read.format("delta").load(rolesPath)
    rolesDelta.select("*").show
    assert( rolesDelta.select("*").count == 4 )
  }
}