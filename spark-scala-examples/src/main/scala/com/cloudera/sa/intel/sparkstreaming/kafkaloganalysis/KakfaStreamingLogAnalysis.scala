package com.cloudera.sa.intel.sparkstreaming.kafkaloganalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.apache.spark.sql.{SQLContext,Row}

/**
 * Created by gmedasani on 3/31/16.
 */
object KakfaStreamingLogAnalysis {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KakfaStreamingLogAnalysis <batch-duration-in-seconds> <zookeeper-quorum> <kafka-topic> <kafka-consumer-group>")
      System.exit(1)
    }

    val batchDuration = args(0)
    val zkQuorum = args(1)
    val kakfaTopics = List((args(2),1)).toMap
    val consumerGroup = args(3)

    //Create Spark configuration and create a streaming context
    val sparkConf = new SparkConf().setAppName("Streaming Log Analyzer").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchDuration.toLong))

    //Create the Kafka stream
    val logsStream = KafkaUtils.createStream(ssc,zkQuorum,consumerGroup,kakfaTopics)

    //Parse the logs in DStream
    val APACHE_ACCESS_LOG_PATTERN_NEW = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)"""
    val parsedLogsStream = logsStream.map(logline => ApacheLogParserUtil.parseRecord(APACHE_ACCESS_LOG_PATTERN_NEW,logline._2))

    //print the parsed log stream
    parsedLogsStream.print()

    val apacheLogRecordStream = parsedLogsStream.map(logRecord => {ApacheLogRecord(logRecord._1,logRecord._2,logRecord._3,logRecord._4,
      logRecord._5,logRecord._6,logRecord._7,logRecord._8,logRecord._9)})

    //create a sqlContext
    val sqlContext = new SQLContext(sc)

    //Sample Analysis
    def getSimpleStatistics(logsRDD:RDD[ApacheLogRecord]):Unit = {
      val logsDF = sqlContext.createDataFrame(logsRDD)
      logsDF.describe("contentSize")show()
    }

    apacheLogRecordStream.foreachRDD(rdd => getSimpleStatistics(rdd))

    //Response Code Analysis
    def getResponseCodeCounts(logsRDD:RDD[ApacheLogRecord]):List[Row] = {
      val logsDF = sqlContext.createDataFrame(logsRDD)
      val responseCodeCountDF = logsDF.groupBy("responseCode").count()
      val responseCodeCountDFNew = responseCodeCountDF.withColumnRenamed("count","responseCode_count")
      val responseCodeCountList = responseCodeCountDFNew.sort("responseCode_count").collect().toList
      responseCodeCountList
    }

    val numberOfDistinctReponseCodes =apacheLogRecordStream.foreachRDD(rdd => {
      val responseCodeCountsList = getResponseCodeCounts(rdd)
      responseCodeCountsList.foreach(println)}
    )


    //Calculate responseCode percentages
    def getResponseCodePercentages(logsRDD:RDD[ApacheLogRecord]) = {
      val logsDF = sqlContext.createDataFrame(logsRDD)
      val responseCodeCountDF = logsDF.groupBy("responseCode").count()
      val responseCodeCountDFNew = responseCodeCountDF.withColumnRenamed("count","responseCode_count")
      val totalResponseCodeCount = responseCodeCountDFNew.select("responseCode_count").groupBy().sum("responseCode_count").collect()(0)(0)
      responseCodeCountDFNew.registerTempTable("responseCountsTable")
      val responseCodePercentDF = sqlContext.sql(s"select responseCode,(responseCode_count/$totalResponseCodeCount)*100 as responseCode_Percentage from responseCountsTable ")
      responseCodePercentDF.collect().toList
    }

    apacheLogRecordStream.foreachRDD(rdd => {
      getResponseCodePercentages(rdd).foreach(println)
    })

    //Start the streaming context and wait until the job is complete
    ssc.start()
    ssc.awaitTermination()
  }
}


case class ApacheLogRecord(host: String,
                           clientID:String,
                           userID:String,
                           dateTime:String,
                           method:String,
                           endPoint:String,
                           protocol:String,
                           responseCode:String,
                           contentSize:Long)
