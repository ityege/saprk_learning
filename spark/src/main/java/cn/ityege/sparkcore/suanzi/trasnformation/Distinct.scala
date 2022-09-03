package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    //distinct所有分区去重
    val c: RDD[String] = sparkContext.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
    val distinct1: RDD[String] = c.distinct(2)
    val collect1: Array[String] = distinct1.collect()
    println(collect1.mkString(","))
    val a: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val length: Int = a.distinct(2).partitions.length
    System.out.print(length)
  }
}
