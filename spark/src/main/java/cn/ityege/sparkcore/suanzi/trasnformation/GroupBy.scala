package cn.ityege.sparkcore.suanzi.trasnformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupBy {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val a: RDD[Int] = sparkContext.parallelize(1 to 9, 3)
    //groupBy根据传入的函数进行分组,返回的结构是一个数字,数组每一个元素是一个元组,key是函数的结果,value这个结构的迭代器
    val groupBy: RDD[(String, Iterable[Int])] = a.groupBy(x => {
      if (x % 2 == 0) "even" else "odd"
    })
    val collect: Array[(String, Iterable[Int])] = groupBy.collect()
    collect.foreach(x => {
      println(x._1 + ":" + x._2)
    })
    val b: RDD[Int] = sparkContext.parallelize(1 to 9, 3)

    def myfunc(a: Int): Int = {
      a % 2
    }

    val groupByb: RDD[(Int, Iterable[Int])] = b.groupBy(myfunc)
    val collectb: Array[(Int, Iterable[Int])] = groupByb.collect()
    collectb.foreach(x => {
      println(x._1 + ":" + x._2)
    })

    val c: RDD[Int] = sparkContext.parallelize(1 to 9, 3)

    def myfun1(a: Int): Int = {
      a % 2
    }

    val collectc: Array[(Int, Iterable[Int])] = c.groupBy(myfun1(_), 1).collect()
    collectc.foreach(x => {
      println(x._1 + ":" + x._2)
    })
  }
}
