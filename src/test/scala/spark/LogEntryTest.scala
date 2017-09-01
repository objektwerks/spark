package spark

import org.scalatest.FunSuite

class LogEntryTest extends FunSuite {
  test ("regex") {
    import LogEntryParser._
    println(logEntryPattern)
  }
}