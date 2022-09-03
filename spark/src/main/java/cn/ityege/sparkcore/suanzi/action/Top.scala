package cn.ityege.sparkcore.suanzi.action

import org.apache.spark.{SparkConf, SparkContext}

object Top {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val c = sparkContext.parallelize(Array(1,3,2,4,9,2,11,5),3)
    val ints: Array[Int] = c.top(3)
    ints.foreach(println(_))
  }
}
