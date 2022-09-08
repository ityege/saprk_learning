package cn.ityege.sparkcore.suanzi.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Count4_10 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("count")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val streamContext: StreamingContext = new StreamingContext(sparkContext, Seconds(1))
    val lines: DStream[String] = streamContext.socketTextStream("nc", 9999)
    val words: DStream[String] = lines.flatMap(line => line.split(" "))
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
    wordCounts.print()
    streamContext.start()
    streamContext.awaitTermination()
  }
}
