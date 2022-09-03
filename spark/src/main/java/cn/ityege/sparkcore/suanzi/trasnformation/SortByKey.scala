package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a = sparkContext.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val b = sparkContext.parallelize(1 to a.count.toInt, 2)
    //a.count 得到单词的字母个数
    val c: RDD[(String, Int)] = a.zip(b)
    //对数据按照key来排序,true为升序,false为降序
    val sortByKey: RDD[(String, Int)] = c.sortByKey(true)
    val collectA: Array[(String, Int)] = sortByKey.collect()
    println(collectA.mkString(","))
    val sortBykey2: RDD[(String, Int)] = c.sortByKey(false)
    val collectB: Array[(String, Int)] = sortBykey2.collect()
    println(collectB.mkString(","))
  }

}
