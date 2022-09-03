package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[String] = sparkContext.parallelize(List("dcp", "fjg", "snn", "wc", "zq"), 2)
    val b: RDD[(Int, String)] = a.map(x => (x.length, x))
    val c: RDD[(Int, String)] = b.reduceByKey((a, b) => a + "字符串拼接" + b)
    val collect: Array[(Int, String)] = c.collect()
    println(collect.mkString(","))
    val d: RDD[Int] = sparkContext.parallelize(List(3, 12, 124, 32, 5), 2)
    val e: RDD[(Int, Int)] = d.map(x => (x.toString.length, x))
    val reduceByKey: RDD[(Int, Int)] = e.reduceByKey(_ + _)
    val collectf: Array[(Int, Int)] = reduceByKey.collect()
    println(collectf.mkString(","))
  }

}
