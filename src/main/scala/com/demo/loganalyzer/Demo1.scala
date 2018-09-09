package com.demo.loganalyzer


import org.apache.spark.{SparkConf, SparkContext}


object Demo1 extends TransformMapper {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val rawData = sc.textFile(args(0))
    // val rawData = sc.textFile("C:\\sparkscala\\MyData\\console")

    val mapRawLine = MapRawLine

    // val option = "Count"
    // val logName = "ERROR"

    val option = args(2)
    val logName =args(3)

    option match {
      case "Count" =>

        val parseData = rawData.flatMap(x => mapRawLine.mapRawLine(x).filter(x=>x.address=="["+ logName +"]"))
        // val output = "C:\\sparkscala\\MyData\\logoutput"
        // transformDate(parseData,output)
        transformCount(parseData,args(1))
      case "DateOrder"=>
        val parseData = rawData.flatMap(x => mapRawLine.mapRawLine(x).filter(x=>x.address=="["+ logName +"]"))
        // val output = "C:\\sparkscala\\MyData\\logoutput"
        // transformDate(parseData,output)
        transformDate(parseData,args(1))

    }
    // println(mapRawData.mapRawLine("2018-09-07 11:04:47 [Information] Delete existing package information"))
    // parseData.saveAsTextFile("C:\\sparkscala\\MyData\\logoutput")


  }
}
