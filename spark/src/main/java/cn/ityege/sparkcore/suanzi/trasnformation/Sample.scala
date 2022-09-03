package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sample {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[Int] = sparkContext.parallelize(1 to 200, 2)
    val collecta: Array[Int] = a.sample(false, 0.1, 0).collect()
    //    10,39,41,53,54,58,60,80,89,98,114,122,136,147,150,157,160,167,175,195
    println(collecta.mkString(","))
    val collectb: Array[Int] = a.sample(true, 0.1, 0).collect()
    //     10,23,25,35,50,68,69,79,79,85,91,91,115,126,129,150,171,187,187,197
    println(collectb.mkString(","))
  }

}
