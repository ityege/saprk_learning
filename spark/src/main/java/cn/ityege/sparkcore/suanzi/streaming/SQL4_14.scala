package cn.ityege.sparkcore.suanzi.streaming

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object SQL4_14 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("count")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val streamContext: StreamingContext = new StreamingContext(sparkContext, Seconds(5))
    val lines: DStream[String] = streamContext.socketTextStream("nc", 9999)
    val words: DStream[String] = lines.flatMap(line => line.split(" "))
    words.foreachRDD(rdd => {
      val sqlContext: SQLContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._
      val wordsDataFrame: DataFrame = rdd.toDF("word")
      wordsDataFrame.registerTempTable("words")
      val wordCountsDataFrame: DataFrame = sqlContext.sql("select word,count(*) as total from words group by word")
      wordCountsDataFrame.show()
    })
    streamContext.start()
    streamContext.awaitTermination()
  }
}
