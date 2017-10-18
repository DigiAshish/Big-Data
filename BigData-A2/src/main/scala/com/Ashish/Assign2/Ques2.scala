package com.Ashish.Assign2
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.Row
import java.io._
object Ques2 {
  def main(args: Array[String]) = {
   
  val conf = new SparkConf()
      .setAppName("Ques2")
      .setMaster("local")
  val sc = new SparkContext(conf)
  val input = sc.textFile(args(0)+"/Output_Q1.txt").cache();
  val userData = sc.textFile(args(0)+"/userdata.txt")
  val writer = new PrintWriter(new File(args(0)+"/Output_Q2.txt"))

  val mapInput =input.map(x => x.replace(",","\t")).map(line=>line.split("\\t")).map(x=> (x(2).toInt,(x(0),x(1))))
  val top10Sort = mapInput.sortByKey(false).map(z=>(z._2._1+"\t"+z._2._2+"\t"+z._1)).take(10).map(x=>x.split("\t"))
  val f1Count=top10Sort.map(x=>(x(0),x(2)))
  val f2Count=top10Sort.map(x=>(x(1),x(2)))
  val input4 = sc.parallelize(f1Count)
  val input5 = sc.parallelize(f2Count)
  
  val userDetails = userData.map(y=>y.split(",")).filter(y=>y.size==10).map(y=>(y(0),y(1)+"\t"+y(2)+"\t"+y(3)+","+y(4)+","+y(5)+","+y(6)+","+y(7)))
  val userFrndDetails1 = input4.join(userDetails).map(x=>(x._2._1+"\t"+x._2._2))
  val userFrndDetails2 = input5.join(userDetails).map(x=>(x._2._2))
  val withIndex1 = userFrndDetails1.zipWithIndex
  val indexKey1 = withIndex1.map{case (k,v) => (v,k)}
  val withIndex2 = userFrndDetails2.zipWithIndex
  val indexKey2 = withIndex2.map{case (k,v) => (v,k)}
  for( a <- 0 to 9){
        val finalOp = (indexKey1.lookup(a).mkString(" ")+"\t\t"+indexKey2.lookup(a).mkString(" ")+"\n")
        writer.write(finalOp)	
      }
    writer.close()
    sc.stop;
  } 
}