package spark

case class LogEntry(ip: String,
                    client: String,
                    user: String,
                    dateTime: Option[String],
                    request :String,
                    status: String,
                    bytes: String,
                    referer: String,
                    agent: String)