package com.cloudera.sa.intel.sparkstreaming.kafkaloganalysis

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by gmedasani on 3/31/16.
 */
object KafkaStreamingWindowStateless {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KakfaStreamingLogAnalysis <batch-duration-in-seconds> <zookeeper-quorum> <kafka-topic> <kafka-consumer-group>")
      System.exit(1)
    }

    val batchDuration = args(0).toLong
    val zkQuorum = args(1)
    val kafkaTopics = List((args(2),1)).toMap
    val consumerGroup = args(3)

    //Create Spark configuration and create a streaming context
    val sparkConf = new SparkConf().setAppName("Streaming Log Analyzer").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchDuration.toLong))

    //Create the Kafka stream
    val logsStream = KafkaUtils.createStream(ssc,zkQuorum,consumerGroup,kafkaTopics)

    //Parse the logs in DStream
    val APACHE_ACCESS_LOG_PATTERN_NEW = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)"""
    val parsedLogsStream = logsStream.map(logline => ApacheLogParserUtil.parseRecord(APACHE_ACCESS_LOG_PATTERN_NEW,logline._2))
    val successfulLogStream = parsedLogsStream.filter(logRecord => logRecord._8 == "200")
    val clientEndPointsPairStream = successfulLogStream.map(request => (request._1, request._6))

    //Create a window on the clientEndPoint DStream to track the user sessions for the last 3 batches.
    val windowDuration = batchDuration*3
    val slidingWindowInterval = batchDuration*2
    val clientEndPointWindow = clientEndPointsPairStream.window(Seconds(windowDuration),Seconds(slidingWindowInterval))
    clientEndPointWindow.count().print()

    //Set the checkpoint directory
//    ssc.checkpoint("file:///Users/gmedasani/Documents/cloudera/Projects/Intel/Intel-AdvancedAnalytics/scratch-data/")

    //start the streaming context
    ssc.start()
    ssc.awaitTermination()

  }

}
