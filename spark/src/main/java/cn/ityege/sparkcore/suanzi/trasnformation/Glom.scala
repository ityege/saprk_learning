package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Glom {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Glom")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val src: RDD[Int] = sparkContext.parallelize(1 to 99, 3)
    //glom返回一个数组，数组里面还是数组，每一个数组是一个分区的元素
    val glom: RDD[Array[Int]] = src.glom
    val collect: Array[Array[Int]] = glom.collect()
    println(collect)
  }
}
