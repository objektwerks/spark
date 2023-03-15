package objektwerks

import SparkInstance._
import Person.personStructType

object DeltaLakeApp extends App {
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
    .awaitTermination(3000)

  val rolesDelta = sparkSession
    .read
    .format("delta")
    .load(rolesPath)
  rolesDelta.select("*").show
  assert( rolesDelta.select("*").count == 4 )
}