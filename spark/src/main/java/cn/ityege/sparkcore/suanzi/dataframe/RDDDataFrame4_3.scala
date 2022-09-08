package cn.ityege.sparkcore.suanzi.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object RDDDataFrame4_3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataFrame")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    val people: RDD[String] = sparkContext.textFile("person.txt")
    val schemaString = "name age"
    val schema = StructType(
      schemaString.split(" ")
        .map(fileName => StructField(fileName, StringType, true))
    )
    val rowRDD: RDD[Row] = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val peopleDataFrame: DataFrame = sqlContext.createDataFrame(rowRDD, schema)
    peopleDataFrame.registerTempTable("people")
    val result: DataFrame = sqlContext.sql("select *  from people")
    import sqlContext.implicits._
    result.map(row=>"Name: "+row(0)+"    Age: "+row(1)).collect().foreach(println)
  }
}
