package com.demo.loganalyzer


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}


object Demo1 extends TransformMapper {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setIfMissing("spark.master", "local")
    conf.setIfMissing("spark.app.name", "LogAnalyzer")

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

     val rawData = sc.textFile(args(0))
    //val rawData = sc.textFile("hdfs://localhost:9000/console.log")

    val outputPath = new Path(args(1))

    //val outputPath = new Path("logoutput")

    val hadoopConf = sc.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)

    try {
      hdfs.delete(outputPath, true)
    } catch {
      case e: Exception =>
        println("Exception occurred while deleting HDFS file => " + e)
    }


    val mapRawLine = MapRawLine

    val option = "Count"
    val logName = "Information"

    // val option = args(2)
    //val logName =args(3)

    option match {
      case "Count" =>

        val parseData = rawData.flatMap(x => mapRawLine.mapRawLine(x).filter(x => x.address == "[" + logName + "]"))
       // val output = "hdfs://localhost:9000/logoutput"
        transformCount(parseData, args(1))
      //transformCount(parseData,args(1))
      case "DateOrder" =>
        val parseData = rawData.flatMap(x => mapRawLine.mapRawLine(x).filter(x => x.address == "[" + logName + "]"))
        // val output = "C:\\sparkscala\\MyData\\logoutput"
        // transformDate(parseData,output)
        transformDate(parseData, args(1))

    }
  }
}
