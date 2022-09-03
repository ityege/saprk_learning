package cn.ityege.sparkcore.suanzi.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Take {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[String] = sparkContext.parallelize(List("a", "b", "c", "d", "e"), 2)
    val strings: Array[String] = a.take(2)
    strings.foreach(println(_))
  }
}
