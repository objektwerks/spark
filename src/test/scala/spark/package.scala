import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream

package object spark {
  val regex = "\\W+"

  def countWords(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd.flatMap(l => l.split(regex))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(w => (w, 1))
      .reduceByKey(_ + _)
  }

  def countWords(ds: DStream[String]): DStream[(String, Int)] = {
    ds.flatMap(l => l.split(regex))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(w => (w, 1))
      .reduceByKey(_ + _)
  }

  def countWords(ds: DStream[String], windowLengthInMillis: Int, slideIntervalInMillis: Int): DStream[(String, Int)] = {
    ds.flatMap(l => l.split(regex))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(w => (w, 1))
      .reduceByKeyAndWindow((x:Int, y:Int) => x + y, Milliseconds(windowLengthInMillis), Milliseconds(slideIntervalInMillis))
  }
}