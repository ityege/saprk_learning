package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[String] = sparkContext.parallelize(List("xwc", "fjg", "wc", "dcp", "zq", "snn", "mk", "zl", "hk", "lp"), 2)
    val b: RDD[Int] = sparkContext.parallelize(List(1, 2, 2, 3, 2, 1, 2, 2, 2, 3), 2)
    val c: RDD[(Int, String)] = b.zip(a)
    val combineByKey: RDD[(Int, List[String])] = c.combineByKey(List(_), (x: List[String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)
    val collect: Array[(Int, List[String])] = combineByKey.collect()
    collect.foreach(x => {
      println(x._1 + ":" + x._2)
    })

  }

}
