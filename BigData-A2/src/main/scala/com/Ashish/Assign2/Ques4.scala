package com.Ashish.Assign2
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import java.io._
object Ques4 {
  def main(args: Array[String]) = {
   
  val conf = new SparkConf()
      .setAppName("Ques4")
      .setMaster("local")
  val sc = new SparkContext(conf)
  val input = sc.textFile(args(0)+"/business.csv").cache();
  val Business_Dataset = input.distinct().map(record => record.split("::"))
  val Business_Dataset_1=Business_Dataset.map(a=>(a(0),a(1).toString+a(2).toString))
  val input1 = sc.textFile(args(0)+"/review.csv").cache();
  val Review_Dataset = input1.distinct().map(record => record.split("::"))

   val filtered_Business_Dataset = Business_Dataset.filter(record => record(1).contains("CA")).filter(record => record(1).contains("Palo Alto")).map(record=>(record(0).toString,record(1).toString))

  val Review_map = Review_Dataset.map(record =>(record(2).toString,(record(1).toString,record(3).toDouble)))

  val result = Review_map.join(filtered_Business_Dataset)
  result.collect.foreach(record => println(record._2._1))
  var onlyResult=result.map(record => record._2._1)
  sc.parallelize(onlyResult.collect.toSeq).saveAsTextFile(args(1))
  }
}