import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ConnectReceiver extends App {

  val sparkConf = new SparkConf().setAppName("Custom Receiver").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Seconds(5L))

  val stream = ssc.receiverStream(new NearEarthObject)

  stream.print()
  ssc.start()
  ssc.awaitTermination()
}
