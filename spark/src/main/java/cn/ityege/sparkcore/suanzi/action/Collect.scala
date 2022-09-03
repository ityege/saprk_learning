package cn.ityege.sparkcore.suanzi.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Collect {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val c: RDD[String] = sparkContext.parallelize(List("a", "b", "c", "d", "e", "f"), 2)
    val collect: Array[String] = c.collect
    println(collect.mkString(","))
  }

}
