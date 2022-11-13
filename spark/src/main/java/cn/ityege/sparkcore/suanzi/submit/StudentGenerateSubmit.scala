package cn.ityege.sparkcore.suanzi.submit

import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.util.UUID
import scala.util.Random

object StudentGenerateSubmit {
  def main(args: Array[String]): Unit = {

    val cls = List("语文", "数学", "音乐", "英语", "化学", "物理")
    val random = new Random()
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Random]))
    val sc = new SparkContext(conf)

    val source: RDD[Int] = sc.parallelize(Range(0, 100000), 1)


    val student: RDD[String] = source.map(a=>{
      UUID.randomUUID ().toString + "\t" + "张三" + a + "\t" + random.nextInt (100) + "\t" + new Gson().toJson (List (cls (random.nextInt (5) ), cls (random.nextInt (5) ) ) )
    })

    student.saveAsTextFile("hdfs://bigdata1:8020/waibu/student")
  }


}
