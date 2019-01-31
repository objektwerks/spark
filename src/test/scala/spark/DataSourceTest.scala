package spark

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.scalatest.{FunSuite, Matchers}

case class Friend(id: Int, name: String, age: Int, score: Int)

class DataSourceTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("csv") {
    val dataframe: DataFrame = sparkSession.read
      .format("csv")
      .option("delimiter",",")
      .option("inferSchema","true")
      .load("./data/txt/friends.txt")
    dataframe.count shouldBe 500

    val friends: Dataset[Friend] = dataframe.map(r => Friend(r.getInt(0), r.getString(1), r.getInt(2), r.getInt(3)))
    friends.count shouldBe 500
  }

  test("text") {
    val rdd: RDD[String] = sparkContext.textFile("./data/txt/license.txt")
    rdd.count shouldBe 19

    val dataframe: DataFrame = sparkSession.read.text("./data/txt/license.txt")
    dataframe.count shouldBe 19

    val dataset: Dataset[String] = sparkSession.read.textFile("./data/txt/license.txt")
    dataset.count shouldBe 19
  }

  test("json") {
    val dataframe: DataFrame = sparkSession.read.json("./data/person/person.json")
    dataframe.count shouldBe 4

    val dataset: Dataset[Person] = sparkSession.read.json("./data/person/person.json").as[Person]
    dataset.count shouldBe 4
  }

  test("parquet") {
    val parquetFileName = s"${UUID.randomUUID.toString}.person.parquet"
    val dataset: Dataset[Person] = sparkSession.read.json("./data/person/person.json").as[Person]
    dataset.write.parquet(s"./target/$parquetFileName")

    val parquet: Dataset[Person] = dataset.sqlContext.read.parquet(s"./target/$parquetFileName").as[Person]
    parquet.createOrReplaceTempView("persons")

    val resultset: Dataset[Person] = parquet.sqlContext.sql("select * from persons where age >= 21 and age <= 22 order by age").as[Person].cache
    resultset.count shouldBe 2
    resultset.head.name shouldBe "betty"
    resultset.head.age shouldBe 21
  }

  test("jdbc") {
    val persons: Dataset[Person] = readPersonsDatasource()
    val avgAgeByRole: Dataset[AvgAgeByRole] = personsToAvgAgeByRole(persons)
    writeAvgAgeByRoleDatasource(avgAgeByRole)
  }

  private def personsToAvgAgeByRole(persons: Dataset[Person]): Dataset[AvgAgeByRole] = {
    val roleByAge: Dataset[Row] = persons.groupBy("role").avg("age").cache // .as[AvgAgeByRole] couldn't resolve avg(age)!
    roleByAge.show
    roleByAge.map(row => AvgAgeByRole(row.getString(0), row.getDouble(1)))
  }

  private def readPersonsDatasource(): Dataset[Person] = {
    import scalikejdbc._
    Class.forName("org.h2.Driver")
    ConnectionPool.singleton("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "sa")
    implicit val session = AutoSession
    sql"""
          drop table persons if exists;
          create table persons (id int not null, age int not null, name varchar(64) not null, role varchar(64) not null);
          insert into persons values (1, 24, 'fred', 'husband');
          insert into persons values (2, 23, 'wilma', 'wife');
          insert into persons values (3, 22, 'barney', 'husband');
          insert into persons values (4, 21, 'betty', 'wife');
      """.execute.apply
    val persons: Dataset[Person] = sqlContext
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", "persons")
      .load
      .as[Person]
    persons.show
    persons
  }

  private def writeAvgAgeByRoleDatasource(avgAgeByRole: Dataset[AvgAgeByRole]): Unit = {
    import scalikejdbc._
    Class.forName("org.h2.Driver")
    ConnectionPool.singleton("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "sa")
    implicit val session = AutoSession
    sql"""
          drop table avg_age_by_role if exists;
          create table avg_age_by_role (role varchar(64) not null, age double not null);
      """.execute.apply
    avgAgeByRole.show
    avgAgeByRole
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", "avg_age_by_role")
      .save
  }
}