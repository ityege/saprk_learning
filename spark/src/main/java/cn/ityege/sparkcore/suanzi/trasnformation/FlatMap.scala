package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMap {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val src: RDD[Int] = sparkContext.parallelize(1 to 10, 5)
    //flatMap输入一个元素，输出多个元素
    val flatMap: RDD[Int] = src.flatMap(num => 1 to num)
    val collect: Array[Int] = flatMap.collect()
    print(collect.mkString(","))
  }
}
