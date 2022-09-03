package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[String] = sparkContext.parallelize(List("mk", "zq", "xwc", "fjg", "dcp", "snn"), 2)
    val b: RDD[(Int, String)] = a.keyBy(x => x.length)
    // keyBy 方法调用 map(x => (f(x),x))生成键值对
    val collect: Array[(Int, Iterable[String])] = b.groupByKey.collect
    collect.foreach(x => {
      println(x._1 + ":" + x._2)
    })
  }

}
