package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitions {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val src: RDD[Int] = sparkContext.parallelize(1 to 9, 3)
    //mapPartitions输入一个分区元素，元素组成迭代器，输出多个元素，组成迭代器
    val collect: Array[String] = src.mapPartitions(myfunc1).collect()
    println(collect.mkString(","))
  }

  //
  def myfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next()
    while (iter.hasNext) {
      val cur = iter.next()
      res.::=(pre, cur)
      pre = cur
    }
    res.iterator
  }

  def myfunc1[T](iter: Iterator[T]): Iterator[String] = {
    List("a", "b", "c").iterator
  }
}
