package cn.ityege.sparkcore.suanzi.action

import org.apache.spark.{SparkConf, SparkContext}

object Aggregate {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val z = sparkContext.parallelize(List(2, 3, 4, 5, 6, 7), 2)
    val i: Int = z.aggregate(10)(_ + _, _ + _)
    println(i)
  }

}
