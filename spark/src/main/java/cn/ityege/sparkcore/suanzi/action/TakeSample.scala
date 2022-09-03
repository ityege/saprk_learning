package cn.ityege.sparkcore.suanzi.action

import org.apache.spark.{SparkConf, SparkContext}

object TakeSample {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val x = sparkContext.parallelize(1 to 100, 2)
    val ints: Array[Int] = x.takeSample(true, 30, 1)
    println(ints.mkString(","))
  }
}
