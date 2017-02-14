package com.cloudera.sa.intel.sparkstreaming.networkwordcount

import org.apache.spark._
import org.apache.spark.streaming._

/**
 * Created by gmedasani on 3/9/16.
 */
object NetworkWordCountDriver {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCountDriver <batch duration in seconds> <Socket port to listen on> ")
      System.exit(1)
    }

    val batchDuration = args(0).toInt
    val socketPort = args(1).toInt

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(batchDuration))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", socketPort)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }
}
