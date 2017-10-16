package com.Ashish.Assign2
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import java.io._
object Ques1 {
  def main(args: Array[String]) = {
   
  val conf = new SparkConf()
      .setAppName("Ques1")
      .setMaster("local")
  val sc = new SparkContext(conf)
  val writer = new PrintWriter(new File(args(0)+"/Output_Q1.txt"))
  val x=0
  val y=0
  val input = sc.textFile(args(0)+"/LiveJournal.txt").cache();
  val tabInput=input.map(line=>line.split("\\t",-1))
  val length=input.count().toInt
  for(x<-0 to length){
    val a1=tabInput.filter(line=>(x.toString()==line(0))).flatMap(li=>li(1).split(",",-1))
    for(y<-x+1 to length){
      val a2 = tabInput.filter(line =>(y.toString()==line(0))).flatMap(li=>li(1).split(",",-1))
      val output = a2.intersection(a1).filter(li =>li.length()>=1).collect()
      if(output.length>=1){
      val finalOp = (x +","+y+"\t"+output.length+"\n")
      writer.write(finalOp)	
      }
      }
  }
  writer.close()
  sc.stop;
  } 
}