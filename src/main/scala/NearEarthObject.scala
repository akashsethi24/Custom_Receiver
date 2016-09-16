import java.sql.Date

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scalaj.http.Http

class NearEarthObject extends Receiver[String](StorageLevel.MEMORY_ONLY) with Runnable {
  var thread: Thread = _

  var date = new Date(System.currentTimeMillis())

  override def onStart(): Unit = {
    thread = new Thread(this)
    thread.start()
  }

  override def onStop(): Unit = {
    thread.interrupt()
  }

  override def run(): Unit = {

    getNeoData(new Date(System.currentTimeMillis()))
  }

  def getNeoData(date: Date): Unit = {
    val stringDate = date.toString
    val result = Http(s" https://api.nasa.gov/neo/rest/v1/feed?start_date=$stringDate&end_date=$stringDate&api_key=0zqbbt7aJRLgz8u2LuilsPyZHJj9pMkG1fUsjfLT")
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .asString.body
    store(result)
    getNeoData(new Date(date.getTime - 86400000))
  }
}
