package com.Ashish.Assign2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import java.io._
object Ques2 {
  def main(args: Array[String]) = {
   
  val conf = new SparkConf()
      .setAppName("Ques2")
      .setMaster("local")
  val sc = new SparkContext(conf)
  val input = sc.textFile(args(0)+"/Output_Q1.txt").cache();
  val userData = sc.textFile(args(0)+"/userdata.txt")
  
  val Top10_Dataset = input.map(line => line.split("\\t"))
  val top10_Data=Top10_Dataset.map(a=>(a(0),a(1).toDouble)).collect()
  val result_final_sort=top10_Data.toList sortWith {_._2 > _._2}
  
  val frndList = result_final_sort.map(y=>y._1.split(",")).map(y=> (y(0),y(1)))
  frndList.foreach(a=>println(a._1,a._2))
  
  val userDetails = userData.map(y=>y.split(",")).filter(y=>y.size==10).map(y=>(y(0),y(2)+"\t"+y(3)+"\t"+y(4)+"\t"+y(5)+"\t"+y(6)+"\t"+y(7)+"\t"+y(8)))
  //val userFrndDetails = frndList.join(userDetails).map(x=>(x._2._1,x._2._2))
  userDetails.foreach(a=>println(a._1,a._2))
  } 
}