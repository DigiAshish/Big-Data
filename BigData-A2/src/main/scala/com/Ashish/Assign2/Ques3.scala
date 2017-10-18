package com.Ashish.Assign2
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import java.io._
object Ques3 {
  def main(args: Array[String]) = {
   
  val conf = new SparkConf()
      .setAppName("Ques3")
      .setMaster("local")
  val sc = new SparkContext(conf)
  val input = sc.textFile(args(0)+"/business.csv").cache();
  val Business_Dataset = input.map(line => line.split("::"))
  val Business_Dataset_1=Business_Dataset.map(a=>(a(0),a(1).toString+a(2).toString))
  val input1 = sc.textFile(args(0)+"/review.csv").cache();
  val Review_Dataset = input1.map(line => line.split("::"))
  val sum=Review_Dataset.map(a=>(a(2),a(3).toDouble)).reduceByKey((a,b)=>a+b).distinct
  val count=Review_Dataset.map(a=>(a(2),1)).reduceByKey((a,b)=>a+b).distinct
  val join_result=sum.join(count)
  
  val rating_avg=join_result.map(a=>(a._1,a._2._2)) 
  val result_final=Business_Dataset_1.join(rating_avg).distinct.collect()

  val result_final_sort=result_final.sortWith(_._2._2>_._2._2).take(10)
  result_final_sort.foreach(a=>println(a._1,a._2._1,a._2._2))
  sc.parallelize(result_final_sort.toSeq).saveAsTextFile(args(1))
  }
}