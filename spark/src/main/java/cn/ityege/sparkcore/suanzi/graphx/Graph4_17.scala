package cn.ityege.sparkcore.suanzi.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Graph4_17 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Graph")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")
    // 为点集创建 RDD
    val users: RDD[(VertexId, (String, String))] =
      sparkContext.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // 为边集创建 RDD
    val relationships: RDD[Edge[String]] =
      sparkContext.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // 建立初始图
    val graph = Graph(users, relationships, defaultUser)
    // 计算所有博士后用户
    val count1: VertexId = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    println(count1)
    // 计算所有"src > dst"的边
    val count2: VertexId = graph.edges.filter(e => e.srcId > e.dstId).count
    println(count2)
    // 用三元组视图创建 factsRDD
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
  }
}
