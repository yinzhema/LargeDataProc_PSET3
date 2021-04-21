package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.math.pow

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var counter=0
    var remaining_vertices=g_in.vertices.count
    val undirectedGraph=Graph(g_in.vertices,g_in.reverse.edges.union(g_in.edges))
    val random_num=scala.util.Random
    var local_global=undirectedGraph.mapVertices[(Float,Boolean,Boolean,Boolean)]((id,attr)=>(random_num.nextFloat(),false,true,true))
    while (remaining_vertices >= 1L) {
        counter=counter+1
        var local=local_global.mapVertices[(Float,Boolean,Boolean,Boolean)]((id,attr)=>(random_num.nextFloat(),attr._2,attr._3,attr._4))

        var new_mis=local.aggregateMessages[(Float,Boolean,Boolean,Boolean)](msg=> {
          if ((msg.srcAttr._1>msg.dstAttr._1) && (msg.srcAttr._3 == true)) {
            msg.sendToSrc(msg.srcAttr._1,true,msg.srcAttr._3, msg.dstAttr._3)
          } else if ((msg.srcAttr._1<=msg.dstAttr._1)&&(msg.srcAttr._3==true)&&(msg.dstAttr._3 == true)) {
            msg.sendToSrc(msg.srcAttr._1,false,msg.srcAttr._3,msg.dstAttr._3)
          } else {
            msg.sendToSrc(msg.srcAttr._1,msg.srcAttr._2,msg.srcAttr._3,msg.dstAttr._3)
          }
        },
          (a,b)=>{
            if (a._4 == false){
              (b._1,b._2,b._3,b._4)
            } else if (b._4 == false){
              (a._1,a._2,a._3,a._4)
            } else{
              (a._1,a._2 && b._2,a._3,a._4)
            }
          })

        local=Graph(new_mis,local.edges)

        new_mis = local.aggregateMessages[(Float,Boolean,Boolean,Boolean)](msg=> {
          if ((msg.srcAttr._2 || msg.dstAttr._2) && (msg.srcAttr._3==true)) {
            msg.sendToSrc(msg.srcAttr._1,msg.srcAttr._2,false,msg.dstAttr._3)
          } else if ((msg.srcAttr._3==true)) {
            msg.sendToSrc(msg.srcAttr._1,msg.srcAttr._2,true,msg.dstAttr._3)
          } else {
            msg.sendToSrc(msg.srcAttr._1,msg.srcAttr._2,msg.srcAttr._3,msg.dstAttr._3)}
        },
          (a,b)=>(a._1,a._2,a._3 && b._3,a._4))

        local=Graph(new_mis,local.edges)

        local_global=local

        remaining_vertices=local_global.vertices.filter{case (id,attr) => attr._3==true}.count

        println("Number of Remaining Vertices: "+remaining_vertices)
    }
    println(counter)
    val output=local_global.mapVertices[Int]((id,attr)=>(if (attr._2) {1} else {-1}))
    return output
  }

  

  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    val undirectedEdge=g_in.reverse.edges.union(g_in.edges)
    val undirectedGraph=Graph(g_in.vertices,undirectedEdge)
    val InGraph=undirectedGraph.subgraph(vpred=(id,label)=>label==1)
    val OutGraph=undirectedGraph.subgraph(vpred=(id,label)=>label==(-1))
    if (InGraph.edges.count.toInt==0){
      val aggregated=undirectedGraph.aggregateMessages[(Int,Int,Int)](msg=>msg.sendToSrc(1,msg.dstAttr,msg.srcAttr),(a,b)=>(a._2+b._2,a._1+b._1,a._3))
      if (aggregated.filter{ case (element) => (element._2._3==(-1)) && (element._2._1+element._2._2)>1}.count == OutGraph.vertices.count) {
        return true
      }
    }
    return false
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)}).filter{case Edge(a,b,c) => a!=b}
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)}).filter{case Edge(a,b,c) => a!=b}
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
