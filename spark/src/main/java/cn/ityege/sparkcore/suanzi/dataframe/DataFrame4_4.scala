package cn.ityege.sparkcore.suanzi.dataframe


import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrame4_4 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val people: DataFrame = sparkContext.textFile("person.txt")
      .map(line => line.split(","))
      .map(x => Person(x(0), x(1).toInt)).toDF()
    people.write.parquet("people.parquet")

    val parquetFile: DataFrame = sqlContext.read.parquet("people.parquet")
    parquetFile.registerTempTable("parquetFile")
    val teenagers: DataFrame = sqlContext.sql("SELECT * FROM parquetFile where age>=13 and age<=180")
    teenagers.map(teenager=>"Name: "+teenager(0)+"   Age:"+teenager(1)).collect().foreach(println)
  }

}
