package com.cloudera.sa.intel.sparkstreaming.loganalysis

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import ApacheLogParserUtil.parseRecord

/**
 * Created by gmedasani on 3/30/16.
 */
object LogAnalyzerDriver {
  def main(args:Array[String]):Unit = {
    if (args.length < 1) {
      System.err.println("Usage: LogAnalyzer <input-directory>")
      System.exit(1)
    }

    val logDirectory = args(0)
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Streaming Log Analyzer")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val sqlContext = new SQLContext(sc)

    val logData = ssc.textFileStream(logDirectory)
    val APACHE_ACCESS_LOG_PATTERN_NEW = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)"""

    //val parsedLogs = logData.map(logline => parseRecord(APACHE_ACCESS_LOG_PATTERN_NEW,logline))
    //println(parsedLogs.count())
    logData.foreachRDD(rdd => println("YEs YES"))
    ssc.start()
    ssc.awaitTermination()
  }
}
