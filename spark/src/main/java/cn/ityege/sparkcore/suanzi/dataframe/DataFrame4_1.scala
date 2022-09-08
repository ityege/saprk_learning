package cn.ityege.sparkcore.suanzi.dataframe

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrame4_1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val SqlContext: SQLContext = new SQLContext(sparkContext)
    val frame: DataFrame = SqlContext.read.json("person.json")
    frame.show()
    frame.printSchema()
    frame.select("name").show()
    frame.select(frame("name"), frame("age") + 1).show
    frame.filter(frame("age") > 21).show()
    frame.groupBy("age").count().show()
  }

}
