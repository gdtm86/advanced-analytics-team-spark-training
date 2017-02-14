package com.cloudera.sa.intel.spark.nasaloganalysis


import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by gmedasani on 3/18/16.
 */

object NASALogAnalysisDriver {

  def main(args: Array[String]): Unit ={
    if (args.length != 3){
      System.err.println("Usage: NASALogAnalysisDriver <input-logs-path> <output-directory> <application-name>")
      System.exit(1)
    }
    val apacheLogsDirectory = args(0)
    val outputDirectory = args(1)
    val appName = args(2)
    val sparkConf = new SparkConf().setAppName(appName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    //Part 1: Parsing Apache Log File Format
    //(1a) Configuration and initial RDD Creation
    //Load the Logs Data into an RDD
    val logsRDD = sc.textFile(apacheLogsDirectory)

//    val APACHE_ACCESS_LOG_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)"""
//
//    //Parse the logsRDD using ApacheLogParserUtil.parseRecord method.
//    val parsedLogsRDD = logsRDD.map(logline => ApacheLogParserUtil.parseRecord(APACHE_ACCESS_LOG_PATTERN,logline))
//    val parsedLogsCount = parsedLogsRDD.count()
//
//    //Filter the logs that were not parsed successfully
//    val failedLogsRDD = parsedLogsRDD.filter(record => record._2 == "failed")
//    val failedLogsCount = failedLogsRDD.count()
//
//    //Filter the logs that were parsed successfully
//    val successfulLogsRDD = parsedLogsRDD.filter(record => record._2 != "failed")
//    val successfulLogsCount = successfulLogsRDD.count()
//
//    println("Total number of logs parsed: "+ parsedLogsCount.toString)
//    println("Total number of logs parsed - succeeded: "+ successfulLogsCount.toString)
//    println("Total number of logs parsed - failed: "+ failedLogsCount.toString)

    //(1b) Data Cleaning
    //Create a new Apache Log pattern to parse all the logs successfully
    val APACHE_ACCESS_LOG_PATTERN_NEW = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)"""

    val parsedLogsRDD1 = logsRDD.map(logline => ApacheLogParserUtil.parseRecord(APACHE_ACCESS_LOG_PATTERN_NEW,logline))
    val parsedLogsCount1 = parsedLogsRDD1.count()

    val failedLogsRDD1 = parsedLogsRDD1.filter(record => record._2 == "failed")
    val failedLogsCount1 = failedLogsRDD1.count()

    val successfulLogsRDD1 = parsedLogsRDD1.filter(record => record._2 != "failed")
    val successfulLogsCount1 = successfulLogsRDD1.count()

    //Here you can see the failed number of logs to be Zero
    println("New - Total number of logs parsed: "+ parsedLogsCount1.toString)
    println("New - Total number of logs parsed - succeeded: "+ successfulLogsCount1.toString)
    println("New - Total number of logs parsed - failed: "+ failedLogsCount1.toString)


    //Part 2: Sample Analysis on the WebServer Log File
    //(2a) Content-Size Statistics.
    // Calculate the Minimum Content Size, Maximum Content Size, Average Content Size.

    //Average Content Size
    val totalContentSize = successfulLogsRDD1.map(record => (record._9).toFloat).reduce((v1,v2) => v1+v2)
    val totalParsedLogsCount = successfulLogsRDD1.count()
    val averageContentSize = totalContentSize.toFloat / totalParsedLogsCount

    //Minimum Content Size
    val minContentSize = successfulLogsRDD1.map(record => (record._9).toFloat).min()

    //Maximum Content Size
    val maxContentSize = successfulLogsRDD1.map(record => (record._9).toFloat).max()

    println("Average Content Size: "+averageContentSize)
    println("Minimum Content Size: "+minContentSize)
    println("Maximum Context Size: "+maxContentSize)

    //(2b) Response Code Analysis
    //count the number of response codes by type
    val responseCodesCount = successfulLogsRDD1
                                 .map(record => (record._8.toInt,1))
                                 .countByKey()

    //print the response codes by count in descending order
    responseCodesCount.toSeq.sortWith(_._2 > _._2).foreach(println)

    //(2c) Frequent Hosts
    val hostsByCount = successfulLogsRDD1
                          .map(record => (record._1,1))
                          .countByKey()

    //print the frequent hosts by count in descending order (top 10)
    hostsByCount.toSeq.sortWith(_._2 > _._2).take(10).foreach(println)


    //(2d) Top Endpoints
    val endPointsbyCount = successfulLogsRDD1
                              .map(record => (record._6,1))
                              .countByKey()

    //print the endpoints visited most in descending order(top 10)
    endPointsbyCount.toSeq.sortWith(_._2 > _._2).take(10).foreach(println)

    //Part 3: Analyzing Web Server log file.
    //(3a)Top 10 Error End Points
    //Any request that doesn't have the response code of 200 is considered a failed request for our analysis

    val not200RDD = successfulLogsRDD1.filter(record => record._8.toInt != 200)
    val not200RDDCountByEndpoint = not200RDD
                                      .map(record => (record._6,1))
                                      .reduceByKey((v1,v2) => v1+v2)
                                      .takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))

    not200RDDCountByEndpoint.toList.foreach(println)


    //(3b) Number of Unique Hosts
    val hostsRDD = successfulLogsRDD1
                      .map(record => record._1)
    val uniqueHostsRDD = hostsRDD.distinct()
    val totalNumberOfUniqueHosts = uniqueHostsRDD.count()

    println("Total Number of Unique Hosts: "+totalNumberOfUniqueHosts)

    //(3c) Number of Daily Unique Hosts
    val uniqueDayHostPairRDD = successfulLogsRDD1
                            .map(record => (record._4.getDayOfMonth,record._1))
                            .distinct()
    val dailyUniqueHostsCountRDD = uniqueDayHostPairRDD
                                  .map(record => (record._1,1))
                                  .reduceByKey((v1,v2) => v1+v2)
                                  .sortByKey()


    val sampleDailyUniqueHostsCount = dailyUniqueHostsCountRDD.take(10)
    sampleDailyUniqueHostsCount.foreach(println)

    //(3d) Average Number of Daily Requests per hosts ( Hint: Total number of daily requests / Number of daily unique hosts)
    val totalNumberOfRequestsByDayRDD = successfulLogsRDD1
                                          .map(record => (record._4.getDayOfMonth,1))
                                          .reduceByKey((v1,v2) => (v1+v2))
                                          .sortByKey()

    //We will use the dailyUniqueHostsCountRDD from the previous item
    val avgNumOfDailyRequestPerHostRDD =  totalNumberOfRequestsByDayRDD
                                            .join(dailyUniqueHostsCountRDD)
                                            .map(record => (record._1,record._2._1/record._2._2))
                                            .sortByKey()

    avgNumOfDailyRequestPerHostRDD.collect().foreach(println)

    //Part(4) Exploring 404 Response Codes.
    //(4a) Counting 404 Response Codes.
    val badRequestsRDD = successfulLogsRDD1.filter(record => record._8.toInt == 404)
    println("Number of bad records: "+ badRequestsRDD.count())


    //(4b) Listing the Top 20 404 Response Code Endpoints
    val top20BadRequestsEndpoints = badRequestsRDD.map(record => (record._6,1))
                                        .reduceByKey((v1,v2) => v1+v2)
                                        .takeOrdered(20)(Ordering[Int].reverse.on(x=>x._2))

    top20BadRequestsEndpoints.foreach(println)

    //(4c) Listing the Top 20 404 Response Code Hosts
    val top20BadRequestsHosts = badRequestsRDD.map(record => (record._1,1))
                                        .reduceByKey((v1,v2) => (v1+v2))
                                        .takeOrdered(20)(Ordering[Int].reverse.on(x => x._2))

    top20BadRequestsHosts.foreach(println)

    //(4d) Listing 404 Response Codes per Day
    val badRequestsByDay = badRequestsRDD.map(record => (record._4.getDayOfMonth,1))
                                          .reduceByKey((v1,v2) => (v1+v2))
                                          .sortByKey()

    badRequestsByDay.collect().foreach(println)

    //(4e) Top Five Days for 404 Response Code
    badRequestsByDay.takeOrdered(5)(Ordering[Int].reverse.on
      (x => x._2)).foreach(element => println("Top 5 days for 404 Response Code" + element))

    //(4f) Hourly 404 Response Codes
    val badRequestsByHour = badRequestsRDD.map(record => (record._4.getHourOfDay,1))
                                        .reduceByKey((v1,v2) => (v1+v2))
                                        .sortByKey()
    badRequestsByHour.collect().foreach(println)

  }

}
