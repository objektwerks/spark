package spark

import org.scalatest.{FunSuite, Matchers}
import spark.entity.{Person, Task}

class BroadcastTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("broadcast join") {
    val broadcastPersons = sparkContext.broadcast(sparkSession.read.json("./data/person/person.json").as[Person])

    val persons = broadcastPersons.value
    val tasks = sparkSession.read.json("./data/task/task.json").as[Task]

    val joinBy = persons.col("id") === tasks.col("pid")
    val personsTasks = persons.join(tasks, joinBy)

    personsTasks.count shouldBe 4
    personsTasks.show
  }
}