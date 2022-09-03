package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Union {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[Int] = sparkContext.parallelize(1 to 4, 2)
    val b: RDD[Int] = sparkContext.parallelize(2 to 4, 1)
    //union将两个RDD合并
    //    val union:RDD[Int] = a.union(b)
    val union: RDD[Int] = a ++ b
    val collect: Array[Int] = union.collect()
    println(collect.mkString(","))
  }

}
