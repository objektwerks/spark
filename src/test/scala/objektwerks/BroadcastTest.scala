package objektwerks

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BroadcastTest extends AnyFunSuite with Matchers {
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