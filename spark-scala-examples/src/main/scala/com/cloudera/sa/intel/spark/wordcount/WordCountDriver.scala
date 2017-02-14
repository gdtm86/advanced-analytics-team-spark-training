package com.cloudera.sa.intel.spark.wordcount

import org.apache.spark.{SparkContext, SparkConf}
import java.util.regex.{Pattern}

/**
 * Created by gmedasani on 3/18/16.
 */
object WordCountDriver {

  def main(args:Array[String]): Unit ={
    if (args.length != 3){
      System.err.println("Usage: WordCountDriver <input-text-path> <output-directory> <application-name>")
      System.exit(1)
    }

    val shakespeareFilePath = args(0)
    val outputDirectory = args(1)
    val appName = args(2)
    val sparkConf = new SparkConf().setAppName(appName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val shakesPeareRDD1  = sc.textFile(shakespeareFilePath)
    println(shakesPeareRDD1.count())

    //Part 1: Create a base RDD and Pair RDDs
    //(1a) Create a base RDD using the strings - "cat","elephant","rat","rat","cat".

    val wordsList = List("cat", "elephant", "rat", "rat", "cat")
    val wordsRDD = sc.parallelize(wordsList, 4)

    //(1b) Pluralize the rdd create above and print
    //function to make plurals given a word
    def makePlural(word:String):String = word + "s"

    val pluralWordsRDD = wordsRDD.map(word => makePlural(word))
    pluralWordsRDD.collect().foreach(println)


    //(1c) Instead of passing the function makePlural, use an anonymous function
    val pluralWordAnanymousRDD = wordsRDD.map(word => word+"s")
    pluralWordsRDD.collect().foreach(println)

    //(1d) Length of each word
    pluralWordsRDD.map(word => word.length).collect().foreach(println)

    //(1e) Create a pair RDD
    val wordsPairRDD = wordsRDD.map(word => (word,1))

    //Part2: Counting with pair RDDs
    //(2a) groupByKey() approach and aggregate locally
    val wordsGrouped = wordsPairRDD.groupByKey()
    val wordsGroupedLocal = wordsGrouped.collect()

    def calculateCount( item:(String,Iterable[Int]) ) = {
      val wordCount = item._2.toList.sum
      (item._1,wordCount)
    }

    for (i <- 0 to wordsGroupedLocal.length-1){
      println(calculateCount(wordsGroupedLocal(i)))
    }


    //(2b) groupByKey() approach and aggregate distributed
    wordsGrouped.map((record) => (record._1,record._2.toList.sum)).collect().foreach(println)

    //(2c) using reduceByKey() approach
    val wordCountsRDD = wordsPairRDD.reduceByKey((v1,v2) => v1+v2)
    wordCountsRDD.collect().foreach(println)

    //Part 3: Finding Unique words and a mean value
    //(3a) Unique words
    println(wordsRDD.distinct().count())

    //(3b) Number of Words per Unique word
    val totalWordCount = wordCountsRDD
      .map(record => record._2)
      .reduce((v1,v2) => v1+v2)

    val average = totalWordCount.toFloat/(wordsRDD.distinct().count())
    println("Average number of words per unique word: "+average)

    //Part 4: Apply word count to a file
    //We will use the complete works of William Shakespeare from Project GutenBerg.

    //(4a) Load the shakespeare text file into a RDD and clean the dataset

    def removePunctuation(text:String):String = {
         /*
         |   Removes punctuation, changes to lower case, and strips leading and trailing spaces.
         |     Note:
         |         Only spaces, letters, and numbers should be retained.  Other characters should should be
         |         eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
         |         punctuation is removed.
         |     Args: text (str): A string.
         |     Returns: str: The cleaned up string.
         |  */

         text.toLowerCase.replaceAll("""[\'?:._!,\(\)\[\];\"-\--\/\}\{]+""","").trim()
      }

    //Apply the removePunctuation on all the entire Shakespeare RDD
    val shakespeareRDD = sc.textFile(shakespeareFilePath).map(record => removePunctuation(record))

    //(4b) Print the first 50 lines with line numbers.

    val shakespeareRDDWithLineNumberRDD = shakespeareRDD.zipWithIndex()
                                            .map(record => record._2.toString+":"+record._1)

    shakespeareRDDWithLineNumberRDD.take(50).foreach(println)

    //(4c) Obtain words from lines.
    //First issue is that we need to split each line by it's spaces
    //The second issue is we need to filter out empty lines.

    val shakespeareWordsRDD = shakespeareRDD.flatMap(line => line.split(' '))
                                            .filter(word => word != "")


    shakespeareWordsRDD.take(10).foreach(println)
    //(4f) Obtain the fifteen most common words.
    val top15WordsAndCounts = shakespeareWordsRDD.map(word => (word,1))
                                                 .reduceByKey((v1,v2) => v1+v2)
                                                 .map(record => (record._2,record._1))
                                                 .sortByKey(false)
                                                 .map(record => (record._2,record._1))
                                                 .take(15)

    top15WordsAndCounts.toList.foreach(record => println(record._1+": "+record._2))


  }
}
