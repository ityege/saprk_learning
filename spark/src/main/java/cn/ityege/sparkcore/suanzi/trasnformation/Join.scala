package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Join {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[String] = sparkContext.parallelize(List("fjg", "wc", "xwc", "dcp"), 2)
    val b: RDD[(Int, String)] = a.keyBy(_.length)
    val c: RDD[String] = sparkContext.parallelize(List("fjg", "wc", "snn", "zq", "xwc", "dcp"), 2)
    val d: RDD[(Int, String)] = c.keyBy(_.length)
    val join: RDD[(Int, (String, String))] = b.join(d)
    val collect1: Array[(Int, (String, String))] = join.collect()
    print(collect1.mkString(","))
  }

}
