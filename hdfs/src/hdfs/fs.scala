package hdfs
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path._
import org.apache.spark._
import SparkContext._

import org.apache.spark.sql.SparkSession

object ScalaHdfs {
  
    def ls(fileSystem:FileSystem,path:String)= {
      println("list path:"+path)
      val fs = fileSystem.listStatus(new Path(path))
      val listPath = FileUtil.stat2Paths(fs)
      for( p <- listPath) {
        println(p)
      }
      println("----------------------------------------")
    }

  def main(args: Array[String]){
    var sconf =new SparkConf().setMaster("local").setAppName("word count")
     println(sconf)
     
     
     
    val conf = new Configuration()
        println(conf)
        val fileSystem = FileSystem.get(conf)
        ls(fileSystem,"/")
        
   
        val spark = SparkSession.builder
      .appName("SparkHdfsLR")
      .getOrCreate()

    val inputPath ="/"
    val lines = spark.read.textFile(inputPath).rdd
    
   print("----lines--" +lines)
   spark.stop()
  }
}