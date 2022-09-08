package cn.ityege.sparkcore.suanzi.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingContext4_9 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(1))

    val lines: DStream[String] = streamingContext.socketTextStream("192.168.184.131", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    words.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
