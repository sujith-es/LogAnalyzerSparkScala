package com.demo.loganalyzer

import org.apache.spark._
import org.joda.time.DateTime

object RunMainJob extends TransformMapper {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-loganalyzer")
    val sc = new SparkContext(conf)
    val startTimeJob = new DateTime(sc.startTime)

    val rawData = sc.textFile("/user/essujit8176/sample.log")
    val numberOfRawLines = rawData.count()

    val mapRawData = MapRawData
    val parseData = rawData.flatMap(x => mapRawData.mapRawLine(x))

    transform(parseData)
  }
}
