package cn.ityege.sparkcore.suanzi

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val src: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)
    //MapPartitionWithIndex输入一个分区的元素，以迭代器的形式，输出多个元素，以迭代器的方式，带索引，这个索引是分区的索引。
    def myfunc(index: Int, iterator: Iterator[Int]): Iterator[String] = {
      iterator.toList.map(x => index + "," + x).iterator
    }
    val collect: Array[String] = src.mapPartitionsWithIndex(myfunc).collect()
    println(collect.mkString(","))
  }

}
