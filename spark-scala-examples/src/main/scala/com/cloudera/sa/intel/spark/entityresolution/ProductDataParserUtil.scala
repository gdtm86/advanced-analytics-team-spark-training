package com.cloudera.sa.intel.spark.entityresolution

/**
 * Created by gmedasani on 3/19/16.
 */
import java.util.regex.Pattern

import org.apache.spark.SparkContext

object ProductDataParserUtil {

  def removeQuotes(text:String):String = {
    text.replaceAll("""\"""","")
  }


  def parseDatafileLine(datafileLine:String):((String,String),Int) = {

    val DATAFILE_PATTERN = """^(.+),(.+),(.*),(.*),(.*)"""
    val compiledPattern = Pattern.compile(DATAFILE_PATTERN)
    val cleanDataFileLine = removeQuotes(datafileLine)
    val matcher = compiledPattern.matcher(cleanDataFileLine)

    if (matcher.find){
      if(matcher.group(1) == "id"){
        val product = matcher.group(2)+" "+matcher.group(3)+" "+matcher.group(4)
        return ((matcher.group(1),product),0)
      }
      val product = matcher.group(2)+" "+matcher.group(3)+" "+matcher.group(4)
      return ((matcher.group(1),product),1)
    }
    return (("failed",cleanDataFileLine),-1)

  }

  def simpleTokenize(text:String):List[String] = {
    val SPLIT_REGEX = """\W+"""
    text.toLowerCase.split(SPLIT_REGEX).toList.filter(element => element != "")
  }

  def tokenize(text:String,stopWords:List[String]):List[String] = {
    val simple_token_list = simpleTokenize(text)
    val filtered_token_list = simple_token_list.filter(token => !stopWords.contains(token))
    filtered_token_list
  }

  def parseGoldFileLine(goldFileLine:String):((String,String),Int) = {
    val GOLD_FILE_PATTERN = """^(.+),(.+)"""
    val compiledPattern = Pattern.compile(GOLD_FILE_PATTERN)
    val cleanGoldFileLine = removeQuotes(goldFileLine)
    val matcher = compiledPattern.matcher(cleanGoldFileLine)

    if (matcher.find){
      if(matcher.group(1) == "idAmazon"){
        val key = matcher.group(1)+" "+matcher.group(2)
        return ((key,"header"),0)
      }
      val key = matcher.group(1)+" "+matcher.group(2)
      return ((key,"gold"),1)
    }
    return ((goldFileLine,"failed"),-1)
  }

}
