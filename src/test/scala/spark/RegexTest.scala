package spark

import java.util.regex.Pattern

import org.scalatest.FunSuite

import scala.util.matching.Regex

class RegexTest extends FunSuite {
  def javaRegex: Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile (regex)
  }

  def scalaRegex: Regex = {
    """
      |(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})? (\S+) (\S+) (\[.+?\]) "(.*?)" (\d{3}) (\S+) "(.*?)" "(.*?)"
    """.stripMargin.r("ip", "client", "user", "dateTime", "request", "status", "bytes", "referer", "agent")
  }

  test ("regex") {
    print (javaRegex)
    print (scalaRegex)
  }
}