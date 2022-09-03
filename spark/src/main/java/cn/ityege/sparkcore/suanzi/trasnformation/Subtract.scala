package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.util

object Subtract {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[Int] = sparkContext.parallelize(1 to 9, 3)
    val b: RDD[Int] = sparkContext.parallelize(1 to 3, 3)
    val c: RDD[Int] = a.subtract(b)
    val collect: Array[Int] = c.collect
    println(collect.mkString(","))
    println(util.Arrays.toString(collect))
  }

}
