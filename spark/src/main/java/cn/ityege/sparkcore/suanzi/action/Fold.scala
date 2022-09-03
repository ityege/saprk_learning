package cn.ityege.sparkcore.suanzi.action

import org.apache.spark.{SparkConf, SparkContext}

object Fold {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)
    val i: Int = a.fold(1)(_ + _)
    println(i)
  }

}
