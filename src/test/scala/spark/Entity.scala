package spark

sealed trait Entity

case class Person(age: Long, name: String) extends Entity