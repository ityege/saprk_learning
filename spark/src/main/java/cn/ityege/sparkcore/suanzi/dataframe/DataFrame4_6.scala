package cn.ityege.sparkcore.suanzi.dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object DataFrame4_6 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val dataFrame1: DataFrame = sparkContext.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    dataFrame1.write.parquet("data/test_table/key=1")
    val dataFrame2: DataFrame = sparkContext.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    dataFrame2.write.parquet("data/test_table/key=2")
    val dataFrame3: DataFrame = sqlContext.read.parquet("data/test_table")
    dataFrame3.printSchema()
  }

}
