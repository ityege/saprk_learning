package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoGroup {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 1), 1)
    val b: RDD[(Int, String)] = a.map(x => (x, "b"))
    val c: RDD[(Int, String)] = a.map(y => (y, "c"))
    val d: RDD[(Int, (Iterable[String], Iterable[String]))] = b.cogroup(c)
    val collect1: Array[(Int, (Iterable[String], Iterable[String]))] = d.collect()
    println(collect1.mkString(","))
    val e: RDD[(Int, String)] = a.map(m => (m, "x"))
    val f: RDD[(Int, (Iterable[String], Iterable[String], Iterable[String]))] = b.cogroup(c, e)
    val collect2: Array[(Int, (Iterable[String], Iterable[String], Iterable[String]))] = f.collect()
    println("+++++++++++++++++++++++++++++++++++++++")
    println(collect2.mkString(","))
  }
}
