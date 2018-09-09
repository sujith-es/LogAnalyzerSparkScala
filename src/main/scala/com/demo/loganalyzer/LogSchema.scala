package com.demo.loganalyzer

import org.apache.spark.rdd.RDD

case class LogSchema(address: String,
                     datetime: String,
                     action: String
                    )

class TransformMapper {
  def transform(events: RDD[LogSchema]) = {
    val e = events.map(x => (x.datetime, 1)).reduceByKey { case (x, y) => x + y }
    e.saveAsTextFile("/user/essujit8176/logoutput/")
  }

  def transformDate(events: RDD[LogSchema], outPath: String) = {
    val e = events.map(x => (x.datetime, 1)).reduceByKey(_ + _).sortByKey(false)
    e.saveAsTextFile(outPath)
  }

  def transformCount(events: RDD[LogSchema], outPath: String) = {
    val e = events.map(x => (x.address, 1)).reduceByKey(_ + _)
    e.saveAsTextFile(outPath)
  }
}

object MapRawLine extends Serializable {
  def mapRawLine(line: String): Option[LogSchema] = {
    val linesample = line
    try {
      val fields = line.split(" ")
      fields.foreach(x => print(x))

      Some(
        LogSchema(
          address = fields(2),
          datetime = fields(0), // fields(1).substring(1, 2),
          action = line
        )
      )

    }
    catch {
      case e: Exception =>
        None
    }
  }
}

object MapRawData extends Serializable {
  def mapRawLine(line: String): Option[LogSchema] = {
    try {
      val fields = line.split(",", -1).map(_.trim)
      Some(
        LogSchema(
          address = fields(0),
          datetime = fields(1).substring(13, 15),
          action = line
        )
      )
    }
    catch {
      case e: Exception =>
        None
    }
  }
}