package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class DataSourceTest extends FunSuite with BeforeAndAfterAll with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  override protected def beforeAll(): Unit = prepareJdbcTestDatabase()

  test("csv") {
    val dataframe: Dataset[Row] = sparkSession.read.csv("./data/txt/friends.txt")
    dataframe.count shouldBe 500

    val uniqueNames: Dataset[String] = dataframe.map(row => row.getString(1)).distinct
    uniqueNames.count shouldBe 30
  }

  test("text") {
    val rdd: RDD[String] = sparkContext.textFile("./data/txt/license.txt")
    rdd.count shouldBe 19

    val dataframe: Dataset[Row] = sparkSession.read.text("./data/txt/license.txt")
    dataframe.count shouldBe 19

    val dataset: Dataset[String] = sparkSession.read.textFile("./data/txt/license.txt")
    dataset.count shouldBe 19
  }

  test("json") {
    val dataframe: Dataset[Row] = sparkSession.read.json("./data/json/person.json")
    dataframe.count shouldBe 4

    val dataset: Dataset[Person] = sparkSession.read.json("./data/json/person.json").as[Person]
    dataset.count shouldBe 4
  }

  test("parquet") {
    val dataset: Dataset[Person] = sparkSession.read.json("./data/json/person.json").as[Person]
    dataset.write.parquet("./target/person.parquet")

    val parquet: Dataset[Person] = dataset.sqlContext.read.parquet("./target/person.parquet").as[Person]
    parquet.createOrReplaceTempView("persons")

    val resultset: Dataset[Person] = parquet.sqlContext.sql("select * from persons where age >= 21 and age <= 22 order by age").as[Person].cache
    resultset.count shouldBe 2
    resultset.head.name shouldBe "betty"
    resultset.head.age shouldBe 21
  }

  test("jdbc") {
    val dataframe: Dataset[Row] = sqlContext
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:./target/testdb")
      .option("user", "test")
      .option("password", "test")
      .option("dbtable", "persons")
      .load()
    dataframe.printSchema
    dataframe.show
    val groupByRole = dataframe.groupBy("role").avg("age").cache
    groupByRole.count shouldBe 2
    groupByRole.collect.map {
      case Row("husband", avgAge) => println(s"Husband average age: $avgAge"); avgAge shouldBe 23.0
      case Row("wife", avgAge) => println(s"Wife average age: $avgAge"); avgAge shouldBe 22.0
    }
  }

  def prepareJdbcTestDatabase(): Unit = {
    import scalikejdbc._
    Class.forName("org.h2.Driver")
    ConnectionPool.singleton("jdbc:h2:./target/testdb", "test", "test")
    implicit val session = AutoSession
    sql"""
          drop table persons if exists;
          create table persons (age int not null, name varchar(64) not null, role varchar(64) not null);
          insert into persons values (24, 'fred', 'husband');
          insert into persons values (23, 'wilma', 'wife');
          insert into persons values (22, 'barney', 'husband');
          insert into persons values (21, 'betty', 'wife');
      """.execute.apply()
  }
}