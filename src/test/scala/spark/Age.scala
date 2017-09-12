package spark

case class Age(age: Long = 0) extends AnyVal {
  implicit def +(other: Age): Age = Age(age + other.age)
}

object Age {
  class Average(ages: Array[Age]) {
    def avg: Age = Age( ages.reduce( ( a, b ) => a + b ).age / ages.length )
  }
  implicit def ordering: Ordering[Age] = Ordering.by(_.age)
  implicit def avg(ages: Array[Age]) = new Average(ages)
  implicit def sum(ages: Array[Age]) = ages.reduce(_ + _)
}