package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[Int] = sparkContext.parallelize(1 to 10, 3)
    val filter1: RDD[Int] = a.filter(x => x % 3 == 0)
    val collect1: Array[Int] = filter1.collect()
    collect1.foreach(x => {
      println(x)
    })
    val b: RDD[Int] = sparkContext.parallelize(1 to 8)
    val collect2: Array[Int] = b.filter(x => x < 4).collect()
    println(collect2.mkString(","))
    val c: RDD[Any] = sparkContext.parallelize(List("cat", "house", 4, 0, 3.5, 2, "dog"))
    //    val collect3: Array[Int] = c.filter(_ < 4).collect()
    //    println(collect3.mkString(","))
  }

}
