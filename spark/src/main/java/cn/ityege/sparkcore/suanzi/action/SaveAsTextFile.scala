package cn.ityege.sparkcore.suanzi.action

import org.apache.spark.{SparkConf, SparkContext}

object SaveAsTextFile {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a = sparkContext.parallelize(1 to 100)
    a.saveAsTextFile("BIT_Spark")
  }
}
