plugins {
    scala
    eclipse
    id("com.github.maiflai.scalatest") version "0.24"
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation("org.scala-lang:scala-library:2.12.8")
    implementation("org.apache.spark:spark-core_2.12:2.4.1")
    implementation("org.apache.spark:spark-streaming_2.12:2.4.1")
    implementation("org.apache.spark:spark-sql_2.12:2.4.1")
    implementation("org.apache.spark:spark-mllib_2.12:2.4.1")
    implementation("org.apache.spark:spark-graphx_2.12:2.4.1")
    implementation("org.scalikejdbc:scalikejdbc_2.12:3.3.3")
    implementation("com.h2database:h2:1.4.197")
    implementation("org.slf4j:slf4j-api:1.7.25")
    testImplementation("org.scalatest:scalatest_2.12:3.0.5")
    testRuntime("org.pegdown:pegdown:1.4.2")
}