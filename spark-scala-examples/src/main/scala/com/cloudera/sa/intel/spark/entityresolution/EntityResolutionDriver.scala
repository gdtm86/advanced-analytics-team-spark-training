package com.cloudera.sa.intel.spark.entityresolution

/**
 * Created by gmedasani on 3/19/16.
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.AccumulatorParam

import scala.collection.mutable.ListBuffer

object EntityResolutionDriver {

  def main(args:Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println("Usage: EntityResolutionDriver <input-data-basedir> <output-data-basedir> <application-name>\"")
      System.exit(1)
    }

    val inputBaseDir = args(0)
    val outputBaseDir = args(1)
    val appName = args(2)
    val sparkConf = new SparkConf().setAppName(appName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    //Part 0: Load the Google_small, Amazon_small, Google and Amazon data sets and clean the data set
    // We read in each of the files Google.csv and Amazon.csv and create RDDs
    val GOOGLE_PATH = inputBaseDir + "/Google.csv"
    val GOOGLE_SMALL_PATH = inputBaseDir + "/Google_small.csv"
    val AMAZON_PATH = inputBaseDir + "/Amazon.csv"
    val AMAZON_SMALL_PATH = inputBaseDir + "/Amazon_small.csv"
    val GOLD_STANDARD_PATH = inputBaseDir + "/Amazon_Google_perfectMapping.csv"
    val STOPWORDS_PATH = inputBaseDir + "/stopwords.txt"

    //utility function to create RDD
    def parseData(fileName: String) = {
      sc.textFile(fileName).map(record => ProductDataParserUtil.parseDatafileLine(record))
    }

    //utility function to create an RDD and filter valid records and failed records
    def loadData(fileName: String): RDD[(String, String)] = {
      val parsedRDD = parseData(fileName)
      val validParsedRDD = parsedRDD.filter(record => record._2 == 1).map(record => record._1)
      val validCount = validParsedRDD.count()
      val rawCount = validCount + 1
      val failedParsedRDD = parsedRDD.filter(record => record._2 == -1).map(record => record._1)
      val failedCount = failedParsedRDD.count()
      println(fileName + ": Read " + rawCount + " lines" + ", successfully parsed " + validCount +
        " lines" + ", failed to parse " + failedCount + " lines")
      return validParsedRDD
    }

    val googleSmallRDD = loadData(GOOGLE_SMALL_PATH).cache()
    val googleRDD = loadData(GOOGLE_PATH).cache()
    val amazonSmallRDD = loadData(AMAZON_SMALL_PATH).cache()
    val amazonRDD = loadData(AMAZON_PATH).cache()

    //Take a sample of google_small and amazon_small datasets and examine
    googleSmallRDD.take(10).foreach(println)
    amazonSmallRDD.take(10).foreach(println)

    // Part 1: Entity Resolution as Text Similarity - Bag of Words
    //(1a) Tokenize the small datasets and print the total token count in google and amazon small datasets
    val stopWords = sc.textFile(STOPWORDS_PATH).collect().toList
    val amazonSmallTokensRDD = amazonSmallRDD.map(record => (record._1, ProductDataParserUtil.tokenize(record._2, stopWords)))
    val googleSmallTokenRDD = googleSmallRDD.map(record => (record._1, ProductDataParserUtil.tokenize(record._2, stopWords)))

    val totalTokenCount = (amazonSmallTokensRDD.map(record => record._2.length).reduce((v1, v2) => v1 + v2)
      + (googleSmallTokenRDD.map(record => record._2.length).reduce((v1, v2) => v1 + v2))
      )

    println("There are " + totalTokenCount + " tokens in the combined Google and Amazon Small datasets")

    amazonSmallTokensRDD.take(10).foreach(println)
    googleSmallTokenRDD.take(10).foreach(println)

    //(1b) Amazon record with most tokens and Google record with most tokens
    def findBiggestRecord(vendorRDD: RDD[(String, List[String])]) = {
      vendorRDD.map(record => (record._1, record._2.length)).takeOrdered(1)(Ordering[Int].reverse.on(record => record._2)).toList
    }

    val amazonSmallBiggestRecord = findBiggestRecord(amazonSmallTokensRDD)
    val googleSmallBiggesRecord = findBiggestRecord(googleSmallTokenRDD)
    println("Biggest Record of Amazon Small dataset: " + amazonSmallBiggestRecord(0)._1 + "," + amazonSmallBiggestRecord(0)._2)
    println("Biggest Record of Google Small dataset: " + googleSmallBiggesRecord(0)._1 + "," + googleSmallBiggesRecord(0)._2)

    //Part 2: ER as Text Similarity - Weighted Bag-of-Words using TF-IDF
    //TF-IDF - Term Frequency/Inverse Document Frequency
    //(2a) Implement a TF function
    def termFrequency(tokens: List[String]) = {
      val totalNumberOfTokens = tokens.length
      val tokenFrequency = tokens.groupBy(identity).mapValues(_.size)
      tokenFrequency.map(record => (record._1, record._2.toFloat / totalNumberOfTokens))
    }

    //(2b) Create a corpusRDD that is a combined dataset of googleSmallTokensRDD and amazonSmallTokensRDD
    val smallCorpusRDD = googleSmallTokenRDD.union(amazonSmallTokensRDD)
    println("Total number of records in amazon and google small tokenRDDs: " + smallCorpusRDD.count())

    //(2c) Implement an IDF Function
    def inverseDocumentFrequency(corpusRDD: RDD[(String, List[String])]): RDD[(String, Float)] = {
      val total_no_of_documents = corpusRDD.count()
      val tokenDocumentCountPairRDD = corpusRDD.map(record => (record._2.distinct))
        .flatMap(record => record.map(record => (record, 1))).reduceByKey((v1, v2) => v1 + v2)
      tokenDocumentCountPairRDD.map(record => (record._1, total_no_of_documents / record._2.toFloat))
    }

    val idfSmallRDD = inverseDocumentFrequency(smallCorpusRDD)

    println("Total number of unique tokens in google and amazon small datasets: " + idfSmallRDD.count())
    idfSmallRDD.take(10).foreach(println)

    //(2d) Tokens with Smallest IDF
    idfSmallRDD.takeOrdered(10)(Ordering[Float].on(record => record._2)).foreach(println)

    //(2e) Implement a function with TF-IDF
    def tf_Idf(tokens: List[String], idfs: Map[String, Float]): Map[String, Float] = {
      val tfs = termFrequency(tokens)
      val tf_idfs = tfs.map(record => (record._1, record._2 * idfs(record._1)))
      tf_idfs
    }

    val idfSmallWeights = idfSmallRDD.collectAsMap().toMap
    val tf_idfSmallRDD = smallCorpusRDD.map(record => (record._1, tf_Idf(record._2, idfSmallWeights)))
    tf_idfSmallRDD.filter(record => record._1 == "b000hkgj8k").collect().foreach(println)

    //Part 3: Entity Resolution as Text Similarity - Cosine Similarity
    //Now we are ready to do text comparisons in a formal way
    //(3a) Implement a function for Cosine Similarity

    //dot-product function
    def dotProduct(tfidfWeights1: Map[String, Float], tfidfWeights2: Map[String, Float]): Float = {
      var sum: Float = 0
      for (key <- tfidfWeights1.keys) {
        if (tfidfWeights2.contains(key)) {
          sum = sum + (tfidfWeights1(key) * tfidfWeights2(key))
        } else {
          sum = sum + 0
        }
      }
      sum
    }

    //norm function
    def norm(tfidfWeights1: Map[String, Float]): Double = {
      var sum = 0
      val termVectorNorm = math.sqrt(tfidfWeights1.map(record => record._2 * record._2).sum)
      termVectorNorm
    }

    //cosine similarity function with given dot-product and norm
    def cosineSimilarity(tfIdfWeights1: Map[String, Float], tfIdfWeights2: Map[String, Float]): Double = {
      val docNorm1 = norm(tfIdfWeights1)
      val docNorm2 = norm(tfIdfWeights2)
      val docsDotProduct = dotProduct(tfIdfWeights1, tfIdfWeights2)
      val cosineSim = docsDotProduct / (docNorm1 * docNorm2)
      cosineSim
    }

    tf_idfSmallRDD.take(10).foreach(println)

    //(3b) Construct an Catesian Product of all the records in the corpus and remove the duplicates
    val crossSmallRDD = googleSmallTokenRDD.cartesian(amazonSmallTokensRDD)
    println("Number of records in the cartesian production: " + crossSmallRDD.count())
    crossSmallRDD.take(10).foreach(println)

    //Calculate the tf-idfs of the crossSmallRDD
    val crossSmallTfIdfRDD = crossSmallRDD.map(record => (
      (record._1._1, tf_Idf(record._1._2, idfSmallWeights)),
      (record._2._1, tf_Idf(record._2._2, idfSmallWeights))
      ))

    // compute the Cosine Similarity of the crossSmallTFIdfRDD
    val crossSmallCosineSimilarityRDD = crossSmallTfIdfRDD.map(record =>
      (record._1._1, record._2._1, cosineSimilarity(record._1._2, record._2._2)))


    crossSmallCosineSimilarityRDD.filter(record => (record._1 == "http://www.google.com/base/feeds/snippets/17242822440574356561" &
      record._2 == "b000o24l3q")).collect().foreach(println);



    //(3c) Perform Entity Resolution with Broadcast variables
    // The solution in (3c) works well for small datasets, but it requires Spark to (automatically) send the
    // idfsSmallWeights variable to all the workers. If we didn't cache() similarities, then it might have to be
    // recreated if we run similar() multiple times. This would cause Spark to send idfsSmallWeights every time

    val idfSmallWeightsBroadcast  = sc.broadcast(idfSmallWeights)

    //Calculate the TF-IDFs using the idfSmallWeightsBroadcast
    val crossSmallTfIdfBroadcastRDD = crossSmallRDD.map(record => (
      (record._1._1,tf_Idf(record._1._2,idfSmallWeightsBroadcast.value)),
      (record._2._1,tf_Idf(record._2._2,idfSmallWeightsBroadcast.value))
      ))

    val crossSmallCosineSimilarityBroadcastRDD = crossSmallTfIdfBroadcastRDD.map(record =>
      (record._1._1,record._2._1,cosineSimilarity(record._1._2,record._2._2)))

    crossSmallCosineSimilarityBroadcastRDD.filter(record => (record._1 == "http://www.google.com/base/feeds/snippets/17242822440574356561" &
      record._2 == "b000o24l3q")).collect().foreach(println)


    //(3d)Perform a Gold Standard Evaluation
    // Now we will load "Gold standard" data and use it to answer several questions.

    val goldRawRDD = sc.textFile(GOLD_STANDARD_PATH)
    val goldParsedRDD = goldRawRDD.map(record => ProductDataParserUtil.parseGoldFileLine(record))
    val goldValidRDD = goldParsedRDD.filter(record => record._2 == 1).map(record => record._1)
    val goldFailedRDD = goldParsedRDD.filter(record => record._2 == -1).map(record => record._1)
    val goldValidCount = goldValidRDD.count()
    val goldRawCount = goldValidCount+1
    val goldFailedCount = goldFailedRDD.count()
    println(GOLD_STANDARD_PATH+": Read " + goldRawCount +" lines"+", successfully parsed "+ goldValidCount+
      " lines"+ ", failed to parse "+ goldFailedCount+" lines")

    goldValidRDD.take(10).foreach(println)

    //Convert crossSmallCosineSimilarityBroadcastRDD into a format that we can join with the goldValidRDD
    val simsSmallRDD = crossSmallCosineSimilarityBroadcastRDD.map(record => (record._2+" "+record._1, record._3))
    simsSmallRDD.take(10).foreach(println)

    //How many true Mapping pairs are there in the small datasets?
    //Any entry that is in the Gold Standard Google Amazon mapping file that is also in the simsRDD means that it is a
    //true mapping pair in small dataset

    val trueMappingSmallRDD = simsSmallRDD.join(goldValidRDD).map(record => (record._1,record._2._1))
    val trueMappingSmallCount = trueMappingSmallRDD.count()

    //What is the average similarity score of true mappings of Amazon and Google Small Datasets
    val trueMappingSmallAvgSimScore = (trueMappingSmallRDD.map(record => record._2).reduce((v1,v2) => v1+v2)/trueMappingSmallCount)

    //What is the average similarity score of the non-true mappings of Amazon and Google Small Datasets
    val nonTrueMappingSmallRDD = simsSmallRDD.subtract(trueMappingSmallRDD)
    val nonTrueMappingSmallCount = nonTrueMappingSmallRDD.count()
    val nonTrueMappingSmallAvgSimScore = (nonTrueMappingSmallRDD.map(record => record._2).reduce((v1,v2) => v1+v2)/nonTrueMappingSmallCount)

    //Print the results
    println("There are a total of "+trueMappingSmallCount+" true mappings in Google and Amazon small dataset")
    println("The average similarity score of true mappings in Google and Amazon small dataset is: "+ trueMappingSmallAvgSimScore)
    println("The average similarity score of non-true mappings in Google and Amazon small " +
      "dataset is: "+ nonTrueMappingSmallAvgSimScore)

    //Part 4: Scalable Entity Resolution

    //An inverted index is a data structure that will allow us to avoid making quadratically many more token comparisons.
    //It maps each token in the dataset to the list of the documents that contain the token. So instead of comparing record
    //by record, each token to every other token to see if they match, we will use inverted indices to lookup records that
    //match on a particular token.

    //Note On Terminology: In Text search, a forward index maps documents in a dataset to the tokens they contain. An
    //inverted index supports the inverse mapping. This was the previous method we used.

    //(4a)Tokenize the full datasets
    val googleTokenRDD = googleRDD.map(record => (record._1, ProductDataParserUtil.tokenize(record._2,stopWords)))
    val amazonTokenRDD = amazonRDD.map(record => (record._1, ProductDataParserUtil.tokenize(record._2,stopWords)))

    //(4b) Compute TFs and IDFS for the full datasets
    val fullCorpusRDD = googleTokenRDD.union(amazonTokenRDD)
    val idfFullRDD = inverseDocumentFrequency(fullCorpusRDD)
    val idfFullWeights = idfFullRDD.collectAsMap().toMap
    val idfFullWeightsBroadcast  = sc.broadcast(idfFullWeights)

    //Pre-compute TF-IDFS. Build Mappings from record ID to weight Vector
//    val tf_idfFullRDD = fullCorpusRDD.map(record => (record._1,tf_Idf(record._2,idfFullWeightsBroadcast.value)))
    val googleTfIdfWeights = googleTokenRDD.map(record => (record._1,tf_Idf(record._2,idfFullWeightsBroadcast.value)))
    val amazonTfIdfWeights = amazonTokenRDD.map(record => (record._1,tf_Idf(record._2,idfFullWeightsBroadcast.value)))
    //Verify the counts
    println(idfFullRDD.count())
    println(googleTokenRDD.count())
    println(amazonTokenRDD.count())

    //(4c) Compute Norms for the weights from the full datasets
    val googleWeightsNorms = googleTfIdfWeights.map(record => (record._1, norm(record._2))).collectAsMap().toMap
    val amazonWeightsNorms = amazonTfIdfWeights.map(record => (record._1, norm(record._2))).collectAsMap().toMap

    //(4d) Create Inverted Indices from full datasets
    def createInverseIndex(record:(String,Map[String,Float])):List[(String, String)] = {
      val tokenDocumentList = ListBuffer[(String,String)]()
      for (key <- record._2.keys){
        tokenDocumentList.append((key,record._1))
      }
      tokenDocumentList.toList
    }

    val amazonInvPairsRDD = amazonTfIdfWeights.flatMap(record => createInverseIndex(record))
    val googleInvPairsRDD = googleTfIdfWeights.flatMap(record => createInverseIndex(record))

    googleInvPairsRDD.take(10).foreach(println)

    println("There are "+ amazonInvPairsRDD.count()+" inverted pairs and "+ googleInvPairsRDD.count()+" inverted pairs.")

    //(4e) Identify common tokens from the full dataset
    //We are now in a position to efficiently perform Entity Resolution on the full datasets. Implement the following algorithm
    //to build a RDD that maps a pair of (ID,URL) to a list of tokens they share in common.

    //* Using thw two inverted indices (RDDs where each element is a pair token and an ID or
    //url that contains that token), create a new RDD that contains only tokens that appear in both datasets.
    // This will yield an RDD of pairs of (token,iterable(Id,URL)).

    //We need a mapping from (ID,URL) to token, so create function that will swap the elements of the RDD you just created
    // to create this new RDD consisting of ((ID,URL),token)

    //Finally,create an RDD consisting of pairs mapping(ID,URL) to all the tokens
    //the pair shares in common.

    val commonInvertedPairsRDD = amazonInvPairsRDD.join(googleInvPairsRDD)
    commonInvertedPairsRDD.take(10).foreach(println)

    val swappedInvertedPairRDD = commonInvertedPairsRDD.map(record => (record._2, record._1))
    swappedInvertedPairRDD.take(10).foreach(println)

    val commonTokensRDD = swappedInvertedPairRDD.groupByKey()
    commonTokensRDD.take(10).foreach(println)

    //(4f)Calculate the similarity score for the common tokens between the two documents.

    val googleTfIdfWeightsBroadcast = sc.broadcast(googleTfIdfWeights.collectAsMap().toMap)
    val amazonTfIdfWeightsBroadcast = sc.broadcast(amazonTfIdfWeights.collectAsMap().toMap)
    val googleWeightsNormsBroadcast = sc.broadcast(googleWeightsNorms)
    val amazonWeightsNormsBroadcast = sc.broadcast(amazonWeightsNorms)

    def fastSimilarityScore(record:((String,String),Iterable[String])):((String,String),Double) = {
      val amazonRec = record._1._1
      val googleRec = record._1._2
      val commonTokens = record._2
      val amazonRecTfIdfWeightsMap = amazonTfIdfWeightsBroadcast.value.get(amazonRec)
      val googleRecTfIdfWeightsMap = googleTfIdfWeightsBroadcast.value.get(googleRec)
      val amazonRecNorm = amazonWeightsNormsBroadcast.value.get(amazonRec)
      val googleRecNorm = googleWeightsNormsBroadcast.value.get(googleRec)
      var weightedSum:Float = 0
      for (token <- commonTokens){
        weightedSum = weightedSum + amazonRecTfIdfWeightsMap.get(token)*googleRecTfIdfWeightsMap.get(token)
      }
      val key = (amazonRec,googleRec)
      val cosineSimilarityScore = weightedSum /(amazonRecNorm.get * googleRecNorm.get)
      (key,cosineSimilarityScore)
    }

    //Create an RDD of mappings from (ID,URL) pair to cosine Similarity Score
    val similaritiesFullRDD = commonTokensRDD.map(record => fastSimilarityScore(record))
    similaritiesFullRDD.take(10).foreach(println)

    println("Total number of Similarities found: "+ similaritiesFullRDD.count())
    val similarityTest = similaritiesFullRDD.filter( record => (record._1._1 == "b00005lzly" & record._1._2 == "http://www.google.com/base/feeds/snippets/13823221823254120257")).collect()
    println(similarityTest(0)._1._1+" , "+similarityTest(0)._1._2+" : "+similarityTest(0)._2)

    //Part 5: Analysis
    //(5a) Counting True Positives, False Positives, and False Negatives
    //Create a similarities RDD that is in the same format as the goldValidRDD
    val simsFullRDD = similaritiesFullRDD.map(record => (record._1._1+" "+ record._1._2, record._2))
    println("There are "+ simsFullRDD.count() + " mappings")

    //RDD consisting of just the similarity scores
    val simsFullValuesRDD = simsFullRDD.map(record => record._2)
    println(simsFullValuesRDD.count())

    //Find the true duplicate pairs and non-duplicate pairs
    val trueMappingsFullRDD = goldValidRDD.leftOuterJoin(simsFullRDD)
    val nonTrueMappingsFullRDD = simsFullRDD.subtractByKey(trueMappingsFullRDD)

    //Gather the similarity scores for the true mappings in the GoldStandard from the full similarities RDD
    val trueMappingsSimsRDD = trueMappingsFullRDD.map(record => record._2._2).cache()
    trueMappingsSimsRDD.take(10).foreach(println)
    println("There are "+ trueMappingsSimsRDD.count() + " true mappings")

    //Gather the similarity scores for the non-true mappings from the full similarities RDD.
    val nonTrueMappingsSimsRDD = nonTrueMappingsFullRDD.map(record => record._2).cache()
    println("There are "+ nonTrueMappingsSimsRDD.count()+  "non true mappings")

    val trueMappingsSimsCleanedRDD = trueMappingsSimsRDD.filter(record => record.getOrElse(0.0) != 0.0)
    trueMappingsSimsCleanedRDD.filter(record => record.getOrElse(0.0) == 0.0).collect().foreach(println)

    //(5b) Let's pick a threshold of 0.5 ( We will first do this for one threshold and later pick a large number of thresholds)
    val threshold = 0.5

    //Calculate the number of false negatives
    //false negatives means -> they are actually positives (since they are in the true mappings dataset), but were thought
    //as negatives since their similarity score is less than the threshold
    val falseNegativesCount = trueMappingsSimsRDD.filter(record => record.getOrElse(0.0) < threshold).count()
    println("Number of False Negatives: "+falseNegativesCount)

    //Calculate the number of true positives
    //True Positives means -> They are actually positives (since they are in the true mappings dataset) and are also thought as positives
    //since their similarity score is greater than the threshold
    val truePositivesCount = trueMappingsSimsRDD.filter(record => record.getOrElse(0.0) > threshold).count()
    println("Number of True Positives: "+ truePositivesCount)

    //Calculate the number of false positives
    //False positives means -> They are not actually positives (since they are not in the true mappings dataset), but they are
    // were wrongfully considered positives since their similarity score is greater than the threshold.
    val falsePositivesCount = nonTrueMappingsSimsRDD.filter(record => record > threshold).count()
    println("Number of False Positives: "+ falsePositivesCount)

    //Calculate Precision -> Precision = True-Positives / (True-positives + false-positives)
    val precision = truePositivesCount.toFloat / (truePositivesCount + falsePositivesCount)

    //Calculate Recall -> Recall = True-Positives/ (True-positives + false-negatives)
    val recall = truePositivesCount.toFloat / (truePositivesCount + falseNegativesCount)

    //Calculate the F-Measure/Score
    // Formula for F-Measure/Score = 2*(precision*recall)/(precision+recall)
    val f_measure = 2 * (precision*recall)/ (precision+recall)

    println("Precision for threshold of "+ threshold+ "is: "+ precision)
    println("Recall for threshold of "+ threshold+ "is: "+ recall)
    println("F-Measure for threshold of "+ threshold+ "is: "+f_measure)

    //(5c)Now let's calculate the F-measure for a threshold of 0.2
    val threshold1 = 0.2
    val falseNegativesCount1 = trueMappingsSimsRDD.filter(record => record.getOrElse(0.0) < threshold1).count()
    println("Number of False Negatives: "+falseNegativesCount1)
    val truePositivesCount1 = trueMappingsSimsRDD.filter(record => record.getOrElse(0.0) > threshold1).count()
    println("Number of True Positives: "+ truePositivesCount1)
    val falsePositivesCount1 = nonTrueMappingsSimsRDD.filter(record => record > threshold1).count()
    println("Number of False Positives: "+ falsePositivesCount1)
    val precision1 = truePositivesCount1.toFloat / (truePositivesCount1 + falsePositivesCount1)
    val recall1 = truePositivesCount1.toFloat / (truePositivesCount1 + falseNegativesCount1)
    val f_measure1 = 2 * (precision1*recall1)/ (precision1+recall1)
    println("Precision for threshold of "+ threshold1 + "is: "+ precision1)
    println("Recall for threshold of "+ threshold1 + "is: "+ recall1)
    println("F-Measure for threshold of "+ threshold1 + "is: "+f_measure1)


    //(5d) Pick 10 thresholds between 0 and 1 can calculate the F-measure for each threshold and list the threshold
    // with the best F-measure

    val falsePositives: ListBuffer[Double] = ListBuffer()
    val truePositives: ListBuffer[Double] = ListBuffer()
    val falseNegatives: ListBuffer[Double] = ListBuffer()
    val precisions: ListBuffer[Double] = ListBuffer()
    val recalls: ListBuffer[Double] = ListBuffer()


    def calculateFMeasure(thresholds:List[Double],trueMappingsSimsRDD: RDD[Option[Double]],
                           nonTrueMappingsSimsRDD: RDD[Double]): Map[Double,Double] = {
      var FMeasures:Map[Double,Double] = Map()
      for (threshold <- thresholds){
        val falseNegativesCount = trueMappingsSimsRDD.filter(record => record.getOrElse(0.0) < threshold).count()
        val truePositivesCount = trueMappingsSimsRDD.filter(record => record.getOrElse(0.0) > threshold).count()
        val falsePositivesCount = nonTrueMappingsSimsRDD.filter(record => record > threshold).count()
        val precision = truePositivesCount.toFloat / (truePositivesCount + falsePositivesCount)
        val recall = truePositivesCount.toFloat / (truePositivesCount + falseNegativesCount)
        val f_measure = (2 * precision * recall)/ (precision + recall)

        FMeasures = FMeasures + (threshold -> f_measure)
        falsePositives.append(falsePositivesCount)
        truePositives.append(truePositivesCount)
        falseNegatives.append(falseNegativesCount)
        precisions.append(precision)
        recalls.append(recall)

      }
      FMeasures
    }
    val thresholds = (1 to 100).map(e => e/100.0).toList
    val F_Measures = calculateFMeasure(thresholds,trueMappingsSimsRDD,nonTrueMappingsSimsRDD)

    println("=========== False Positives =======================")
    falsePositives.toList.foreach(println)
    println("============ True Positives =======================")
    truePositives.toList.foreach(println)
    println("============ False Negatives ======================")
    falseNegatives.toList.foreach(println)
    println("============ Precisions ======================")
    precisions.toList.foreach(println)
    println("============ Recalls ======================")
    recalls.toList.foreach(println)
    println("==============F Measures====================")
    F_Measures.foreach(println)

    println("The best F_measure or F_score is "+F_Measures.maxBy(_._2)._2+" for a threshold value of: " +
      ""+F_Measures.maxBy(_._2)._1  )


    //State of the art tools can get F-measure of about 60% on this dataset. In this exercise, our best F-score is closer to 40%
    // There are several ways we might improve our simple classifier, including
    // * Using additional attributes
    // * Performing better featurization of our data (e.g, stemming, n-grams, etc)
    // * Using different similarity functions

  }
}
