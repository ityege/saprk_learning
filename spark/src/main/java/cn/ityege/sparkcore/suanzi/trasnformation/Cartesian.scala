package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Cartesian {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val x: RDD[Int] = sparkContext.parallelize(List(1, 2, 3), 1)
    val y: RDD[Int] = sparkContext.parallelize(List(4, 5), 1)
    val cartesian: RDD[(Int, Int)] = x.cartesian(y)
    var collect: Array[(Int, Int)] = cartesian.collect()
    println(collect.mkString(","))
  }

}
