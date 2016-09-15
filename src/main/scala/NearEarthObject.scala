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

    while (date.toString != "1900-01-01") {
      val newDate = new Date(date.getTime - 86400000)
      val result = Http(s" https://api.nasa.gov/neo/rest/v1/feed?start_date=${newDate.toString}&end_date=${newDate.toString}&api_key=0zqbbt7aJRLgz8u2LuilsPyZHJj9pMkG1fUsjfLT")
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .asString
      date = newDate
      val response = result.body
      println(date + " HERE")

      store(response)
    }
  }
}
