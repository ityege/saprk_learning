package cn.ityege.sparkcore.suanzi.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object JSON {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    val path: String = "people.json"
    val pople = sqlContext.read.json(path)
    pople.printSchema()
    pople.registerTempTable("people")
    val teenagers: DataFrame = sqlContext.sql("SELECT * from people where age>=13 and age<=100")
    teenagers.show()
    val anotherPeopleRDD: RDD[String] = sparkContext.parallelize("""{"name"："Yin"，"address"：{"city"："Columbus"，"state"："Ohio"}}""" :: Nil)
    val anotherPeople: DataFrame = sqlContext.read.json(anotherPeopleRDD)
    anotherPeople.show()
  }

}
