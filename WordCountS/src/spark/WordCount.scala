package spark

import org.apache.spark._
import SparkContext._
object WordCount {
//  def main(args:Array[String]):Unit= {
//  
//  val conf =new SparkConf().setMaster("local[*]").setAppName("word count")
//  val sc = new SparkContext(conf)
//  val textFile =sc.textFile("/Users/didi/java/WordCount/input/input1.txt")
////val result = textFile.flatMap(line => line.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _)
//  textFile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)
//  }
   def main(args: Array[String]) {
//    var fileName="/Users/didi/java/WordCount/input/input1.txt" 
//     //val conf = new SparkConf()
//    val conf =new SparkConf().setMaster("local").setAppName("word count")
//    //val conf =new SparkConf().setMaster("127.0.0.1:55477").setAppName("word count")
//     val sc = new SparkContext(conf)
//     val line = sc.textFile(fileName)
//
//     line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)
//
//     sc.stop()
     //val conf =new SparkConf().setMaster("local[4]").setAppName("word count")
     //val sc = new SparkContext("local", "WordCount",
    //System.getenv("SPARK_HOME"))
     //val conf = new SparkConf().setAppName("Spakr WordCount written by jaba").setMaster("127.0.0.1:8085");
     val conf = new SparkConf()
             .setMaster("local")
             .setAppName("CountingSheep")
             .set("spark.executor.memory", "1g")
     val sc = new SparkContext(conf)
     //val file=sc.textFile("hdfs://master.Hadoop/input/*.txt")
     //val file=sc.textFile("hdfs://matser:9000/input/*.txt")
      val file=sc.textFile("hdfs://localhost:8020/input/*.txt")
     //val file=sc.textFile("file:///Users/didi/java/WordCount/input/*.txt")
     val count=file.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
    count.collect.foreach(println)
     sc.stop()
 }
}