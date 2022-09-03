package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object PersistCache {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val c: RDD[String] = sparkContext.parallelize(List("a", "b", "c", "d", "e", "f"), 1)
    c.cache()
    val d: RDD[Int] = sparkContext.parallelize(1 to 9, 3)
    d.persist(StorageLevel.MEMORY_ONLY)
  }

}
