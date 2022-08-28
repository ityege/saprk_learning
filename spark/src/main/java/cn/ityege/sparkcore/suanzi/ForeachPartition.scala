package cn.ityege.sparkcore.suanzi

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ForeachPartition {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ForeachPartition")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val src: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    //foreachPartition遍历每一个分区的元素
    //foreach遍历所有分区元素
    src.foreachPartition(x => println(x.reduce((a,b)=>a+b)))
  }
}
