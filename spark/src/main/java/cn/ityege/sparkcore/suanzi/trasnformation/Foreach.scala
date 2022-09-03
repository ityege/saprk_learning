package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Foreach {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val src: RDD[String] = sparkContext.parallelize(List("xwc", "fjg", "wc", "dcp", "zq", "snn", "mk", "zl", "hk", "lp"), 3)
    //Foreach遍历每一个元素
    src.foreach(x => println(x + " are from BIT"))
  }

}
