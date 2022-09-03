package cn.ityege.sparkcore.suanzi.action

import org.apache.spark.{SparkConf, SparkContext}

object CountByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a = sparkContext.parallelize(List((1, "bit"), (2, "xwc"), (2, "fjg"), (3, "wc"), (3, "wc"), (3, "wc")), 2)
    val key: collection.Map[Int, Long] = a.countByKey
    key.foreach(println(_))
    
  }

}
