package objektwerks

import java.util.UUID

import org.apache.spark.sql.{Dataset, SaveMode}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataSourceTest extends AnyFunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("csv") {
    val dataframe = sparkSession
      .read
      .format("csv")
      .option("delimiter",",")
      .option("inferSchema","true")
      .load("./data/txt/friends.txt")
      .cache
    dataframe.count shouldBe 500

    val friends = dataframe.map(row => Friend(row.getInt(0), row.getString(1), row.getInt(2), row.getInt(3)))
    friends.count shouldBe 500
  }

  test("text") {
    val rdd = sparkContext.textFile("./data/txt/license.txt")
    rdd.count shouldBe 19

    val dataframe = sparkSession.read.text("./data/txt/license.txt")
    dataframe.count shouldBe 19

    val dataset = sparkSession.read.textFile("./data/txt/license.txt")
    dataset.count shouldBe 19
  }

  test("json") {
    val dataframe = sparkSession.read.json("./data/person/person.json")
    dataframe.count shouldBe 4

    val dataset = sparkSession.read.json("./data/person/person.json").as[Person]
    dataset.count shouldBe 4
  }

  test("parquet") {
    val parquetFileName = s"person-${UUID.randomUUID.toString}.parquet"
    val dataset = sparkSession.read.json("./data/person/person.json").as[Person]
    dataset.write.parquet(s"./target/$parquetFileName")

    val parquet = sparkSession.read.parquet(s"./target/$parquetFileName").as[Person]
    parquet.createOrReplaceTempView("persons")

    val resultset = sparkSession.sql("select * from persons where age >= 21 and age <= 22 order by age").as[Person].cache
    resultset.count shouldBe 2
    resultset.head.name shouldBe "betty"
    resultset.head.age shouldBe 21
  }

  test("hive") {
    sparkSession.sql("DROP TABLE IF EXISTS keyvalue")
    sparkSession.sql("CREATE TABLE keyvalue (key INT, value INT) row format delimited fields terminated by ','")
    sparkSession.sql("LOAD DATA LOCAL INPATH './data/txt/kv.txt' INTO TABLE keyvalue")
    val keyvalues = sparkSession.sql("SELECT * FROM keyvalue").as[KeyValue].cache
    keyvalues.count shouldBe 9
    keyvalues.filter(_.key == 3).head.value shouldBe 33
    keyvalues.show
  }

  test("jdbc") {
    prepareDatasource shouldBe false  // Prepare

    val persons = readPersonsDatasource  // Source
    val avgAgeByRole = personsToAvgAgeByRole(persons)  // Flow
    writeAvgAgeByRoleDatasource(avgAgeByRole)  // Sink

    val avgAgeByRoles = readAvgAgeByRoleDatasource  // Verify
    avgAgeByRoles.count shouldBe 2
    avgAgeByRoles.show
  }

  private def prepareDatasource: Boolean = {
    import scalikejdbc._
    Class.forName("org.h2.Driver")
    ConnectionPool.singleton("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "sa")
    implicit val session = AutoSession
    sql"""
          drop table persons if exists;
          drop table avg_age_by_role if exists;
          create table persons (id int not null, age int not null, name varchar(64) not null, role varchar(64) not null);
          insert into persons values (1, 24, 'fred', 'husband');
          insert into persons values (2, 23, 'wilma', 'wife');
          insert into persons values (3, 22, 'barney', 'husband');
          insert into persons values (4, 21, 'betty', 'wife');
          create table avg_age_by_role (role varchar(64) not null, avg_age double not null);
      """.execute.apply
  }

  private def readPersonsDatasource: Dataset[Person] = {
    sparkSession
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:test")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", "persons")
      .load
      .as[Person]
  }

  private def personsToAvgAgeByRole(persons: Dataset[Person]): Dataset[AvgAgeByRole] = {
    persons
      .groupBy("role")
      .avg("age")
      .map(row => AvgAgeByRole(row.getString(0), row.getDouble(1)))
  }

  private def writeAvgAgeByRoleDatasource(avgAgeByRole: Dataset[AvgAgeByRole]): Unit = {
    avgAgeByRole
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:test")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", "avg_age_by_role")
      .save
  }

  private def readAvgAgeByRoleDatasource: Dataset[AvgAgeByRole] = {
    sparkSession
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:test")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", "avg_age_by_role")
      .load
      .as[AvgAgeByRole]
      .cache
  }
}