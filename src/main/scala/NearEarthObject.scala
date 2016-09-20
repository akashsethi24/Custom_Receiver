import java.sql.Date

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scalaj.http.Http

class NearEarthObject extends Receiver[String](StorageLevel.MEMORY_ONLY) with Runnable {


  override def onStart(): Unit = {
    val thread = new Thread(this)
    thread.start()
  }

  override def onStop(): Unit = {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  override def run(): Unit = {

    getNeoData(new Date(System.currentTimeMillis()))
  }

  def getNeoData(date: Date): Unit = {
    date match {
      case currentDate if currentDate.toString == "1900-01-01" => store("End Of Data")
      case _ =>
        val stringDate = date.toString
        val result = Http(s" https://api.nasa.gov/neo/rest/v1/feed?start_date=$stringDate&end_date=$stringDate&api_key=0zqbbt7aJRLgz8u2LuilsPyZHJj9pMkG1fUsjfLT")
          .header("Content-Type", "application/json")
          .header("Charset", "UTF-8")
          .asString.body
        store(result)
        getNeoData(new Date(date.getTime - 86400000))
    }
  }
}
