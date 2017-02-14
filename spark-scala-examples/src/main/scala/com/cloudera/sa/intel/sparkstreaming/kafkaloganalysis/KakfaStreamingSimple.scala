package com.cloudera.sa.intel.sparkstreaming.kafkaloganalysis

/**
 * Created by gmedasani on 3/31/16.
 */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object KakfaStreamingSimple {

  def main(args:Array[String]):Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KakfaStreamingSimple <batch-duration-in-seconds> <zookeeper-quorum> <kafka-topic> <kafka-consumer-group>")
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
    val logsStreamCount = logsStream.count()

    //print 10 logs from each DStream batch duration
    logsStream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
