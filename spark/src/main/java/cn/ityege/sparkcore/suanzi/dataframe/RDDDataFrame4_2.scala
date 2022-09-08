package cn.ityege.sparkcore.suanzi.dataframe

import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object RDDDataFrame4_2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

    val people = sparkContext.textFile("D:\\code\\learning\\saprk_learning\\person.txt")
      .map(_.split(","))
      .map(p => Person(p(0), p(1).toInt))
      .toDF()
    people.registerTempTable("people")
    val teenagers: DataFrame = sqlContext.sql("SELECT name,age FROM people where age>18 and age<180")

    teenagers.map(row => row(0) + ":" + row(1)).collect().foreach(println)
    println("+++++++++++++++++++++++++++++++++++++++++++++++++==")
    // 或者通过字段名
    teenagers.map(row => "Name: " + row.getAs[String]("name") + " Age:" + row.getAs[Int]("age"))
      .collect().foreach(println)
    println("+++++++++++++++++++++++++++++++++++++++++++++++++==")
    // row.getValues Map[T]方法一次返回多个列,把列内容放入 Map[String T]中
    teenagers.map(row=>row.getValuesMap[String](List("name"))).collect().foreach(println)
  }
}

case class Person(name: String, age: Int)
