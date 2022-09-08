package cn.ityege.sparkcore.suanzi.dataframe

import org.apache.spark.{SparkConf, SparkContext}

object Hive4_8 {
  //驯服不了
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Hive4_8")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
//    sparkContext.setLogLevel("ERROR")
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
    sqlContext.sql("drop table src")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)" +
      "row format delimited fields terminated by ','")
//    sqlContext.sql("LOAD DATA LOCAL INPATH 'D:\\code\\learning\\saprk_learning\\spark\\src\\main\\resources\\kv1.txt' INTO TABLE src")
    // HiveQL 语法的查询语句
    sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)
  }

}
