package spark

import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.scalatest.{FunSuite, Matchers}
import spark.entity.{KeyValue, Person, PersonsTasks, Task}

class SqlTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("dataframe sql") {
    val dataframe = sparkSession.read.json("./data/person/person.json").cache
    assert(dataframe.isInstanceOf[Dataset[Row]])
    dataframe.count shouldBe 4
    dataframe.createOrReplaceTempView("persons")

    val rows = sqlContext.sql("select * from persons where age >= 21 and age <= 22 order by age").cache
    rows.count shouldBe 2
    rows.head.getLong(0) shouldBe 21
    rows.head.getString(2) shouldBe "betty"

    sqlContext.sql("select min(age) from persons").head.getLong(0) shouldBe 21
    sqlContext.sql("select avg(age) from persons").head.getDouble(0) shouldBe 22.5
    sqlContext.sql("select max(age) from persons").head.getLong(0) shouldBe 24
    sqlContext.sql("select sum(age) from persons").head.getLong(0) shouldBe 90

    val agesLimitByTwoDesc = sqlContext
      .sql("select name, age from persons where role = 'wife' order by name desc limit 2")
    agesLimitByTwoDesc.head.getString(0) shouldBe "wilma"
    agesLimitByTwoDesc.head.getLong(1) shouldBe 23
    agesLimitByTwoDesc.take(2).tail(0).getString(0) shouldBe "betty"
    agesLimitByTwoDesc.take(2).tail(0).getLong(1) shouldBe 21
  }

  test("dataset sql") {
    val dataset = sparkSession.read.json("./data/person/person.json").as[Person].cache
    dataset.count shouldBe 4
    dataset.createOrReplaceTempView("persons")

    val persons = sqlContext.sql("select * from persons where age >= 21 and age <= 22 order by age").as[Person].cache
    persons.count shouldBe 2
    persons.head.age shouldBe 21
    persons.head.name shouldBe "betty"

    sqlContext.sql("select min(age) from persons").as[Long].head shouldBe 21
    sqlContext.sql("select avg(age) from persons").as[Double].head shouldBe 22.5
    sqlContext.sql("select max(age) from persons").as[Long].head shouldBe 24
    sqlContext.sql("select sum(age) from persons").as[Long].head shouldBe 90

    val agesLimitByTwoDesc = sqlContext
      .sql("select name, age from persons where role = 'husband' order by name desc limit 2")
      .as[(String, Long)]
      .collect
    ("fred",24) shouldEqual agesLimitByTwoDesc.head
    ("barney",22) shouldEqual agesLimitByTwoDesc.tail(0)
  }

  test("dataframe join") {
    val persons = sparkSession.read.json("./data/person/person.json").cache
    val tasks = sparkSession.read.json("./data/task/task.json").cache
    persons.count shouldBe 4
    tasks.count shouldBe 4
    persons.createOrReplaceTempView("persons")
    tasks.createOrReplaceTempView("tasks")

    val personsTasks: Dataset[Row] = sqlContext.sql("SELECT * FROM persons, tasks WHERE persons.id = tasks.pid").cache
    personsTasks.count shouldBe 4
    personsTasks.show

    personsTasks.createOrReplaceTempView("persons_tasks")
    val personTask: Dataset[Row] = sqlContext.sql("select name, task from persons_tasks").cache
    personTask.count shouldBe 4
    personTask.show
  }

  test("dataset join") {
    val persons = sparkSession.read.json("./data/person/person.json").as[Person].cache
    val tasks = sparkSession.read.json("./data/task/task.json").as[Task].cache
    persons.count shouldBe 4
    tasks.count shouldBe 4
    persons.createOrReplaceTempView("persons")
    tasks.createOrReplaceTempView("tasks")

    val personsTasks: Dataset[PersonsTasks] = sqlContext.sql("select * from persons, tasks where persons.id = tasks.pid").as[PersonsTasks].cache
    personsTasks.count shouldBe 4
    personsTasks.show

    personsTasks.createOrReplaceTempView("persons_tasks")
    val personTask: Dataset[(String, String)] = sqlContext.sql("select name, task from persons_tasks").as[(String, String)].cache
    personTask.count shouldBe 4
    personTask.show
  }

  test("udf") {
    val cityTemps = sparkSession.read.json("./data/weather/city_temps.json").cache
    cityTemps.createOrReplaceTempView("city_temps")

    sqlContext.udf.register("celciusToFahrenheit", (degreesCelcius: Double) => (degreesCelcius * 9.0 / 5.0) + 32.0)

    val temps = sqlContext.sql("select city, celciusToFahrenheit(avgLow) as avgLowFahrenheit, celciusToFahrenheit(avgHigh) as avgHighFahrenheit from city_temps")
    temps.count shouldBe 6
    temps.show
  }

  test("jdbc") {
    writeKeyValues("key_values", List[KeyValue](KeyValue(1, 1), KeyValue(2, 2), KeyValue(3, 3)).toDS)
    val keyvalues = readKeyValues("key_values").toDF
    keyvalues.createOrReplaceTempView("key_values")
    sqlContext.sql("select count(*) as total_rows from key_values").head.getLong(0) shouldBe 3
    sqlContext.sql("select min(key) as min_key from key_values").head.getInt(0) shouldBe 1
    sqlContext.sql("select max(value) as max_value from key_values").head.getInt(0) shouldBe 3
    sqlContext.sql("select sum(value) as sum_value from key_values").head.getLong(0) shouldBe 6
  }

  private def writeKeyValues(table: String, keyValues: Dataset[KeyValue]): Unit = {
    keyValues
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:kv;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", table)
      .save
  }

  private def readKeyValues(table: String): Dataset[KeyValue] = {
    sqlContext
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:kv;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", table)
      .load
      .as[KeyValue]
  }
}