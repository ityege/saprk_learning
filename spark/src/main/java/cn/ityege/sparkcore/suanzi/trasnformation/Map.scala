package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Map {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val src: RDD[String] = sparkContext.parallelize(List("bit", "linc", "xwc", "fjg", "wc", "spark"), 3)
    // map输入一个元素，输出一个元素。
    val map: RDD[Int] = src.map(word => word.length)
    val zip: RDD[(String, Int)] = src.zip(map)
    val collect: Array[(String, Int)] = zip.collect()
    println(collect.mkString(","))
  }
}
