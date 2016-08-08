package main.scala.master.spark.main

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import main.scala.master.spark.mllib.RandomForestRun
import main.scala.master.spark.preprocessing.PreProcessing
import main.scala.master.spark.mllib.NaiveBayesRun
import main.scala.master.spark.util.BuildArff
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import main.java.com.mestrado.utils.MrUtils
import org.apache.spark.storage.StorageLevel

object MainSpark {

  private var sparkUrl: String = ""
  var clusterUrl: String = ""
  private var user: String = ""
  var num_block: Int = 1
  private var trainFormatedDir: String = ""
  private var testFormatedDir: String = ""
  private var dataset: String = ""
  private var preTime = 0.0
  private var rfTime = 0.0
  private var nbTime = 0.0
  var logDir = "/home/hadoop/petrini";//Dir for time and evaluation logs files
//  var logDir = "/home/hdp/petrini";//Dir for time and evaluation logs files
  var evaluationFile = logDir
  var fold = "1";
  var lambda = 1.0
  var featurePercent = 1.0

  def printConfig() {
    println("\n" + ("*" * 40) + "\n")
    println("----MLlib Naive Bayes----")
    printf("Spark url: %s\n", sparkUrl)
    printf("User dir: %s\n", user)
    printf("HDFS url: %s\n", clusterUrl)
    printf("Dataset: %s\n", dataset)
    printf("Num block: %s\n", num_block)
    printf("Lambda: %f\n",lambda)
    println("\n" + ("*" * 40) + "\n")
  }

  def excludeUsedDirs() {
    println("\n\nExclude used files...")
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(clusterUrl), hadoopConf)
    try {
      val fileName = Array[String](trainFormatedDir, testFormatedDir)
      for (file <- fileName) { println("Deleting..." + file); hdfs.delete(new org.apache.hadoop.fs.Path(file), true) }
    } catch { case _: Throwable => {} }
  }

  def initialConfig(args: Array[String]) {
    println("Print args:")
    for (elem <- args) println(elem)

    if (args.length != 6) {
      println("Error, missing arguments: <num_blocks> <dataset> <master-name> <lambda> <featuresPercent> <fold>")
      System.exit(1)
    } else {
      sparkUrl = "spark://" + args(2) + ":7077"
      clusterUrl = "hdfs://" + args(2) + "/"
      num_block = args(0).toInt
      dataset = args(1)
      user = clusterUrl + "user/hdp/"
      trainFormatedDir = user + "trainFormated";
      testFormatedDir = user + "testFormated";
      lambda = args(3).toDouble
      featurePercent = args(4).toDouble
      fold = args(5)
    }
    excludeUsedDirs()
    printConfig()
  }

  def stop(sc: SparkContext, log: String) {
    sc.stop();
    println(log)
    System.exit(0);
  }

  def main(args: Array[String]): Unit = {
    initialConfig(args)
    var logSb: StringBuilder = new StringBuilder()

    logSb.append("\n\n" + ("*" * 40) + "\n\n")

    val inputTrainFileName: String = user + "input/treino/treino2"
    val inputTestFileName: String = user + "input/teste/teste2"
    val stopWordFileName: String = user + "input/stopwords.ser"
    val conf = new SparkConf().setAppName("First test").setMaster(sparkUrl);
    val sc = new SparkContext(conf)

    preTime = System.currentTimeMillis()
    /*Clean entity descriptions*/
    val datasetTrainClean = PreProcessing.preProcess(inputTrainFileName, stopWordFileName, num_block, sc, clusterUrl)
    datasetTrainClean.persist(StorageLevel.MEMORY_AND_DISK)
    val datasetTestClean = PreProcessing.preProcess(inputTestFileName, stopWordFileName, num_block, sc, clusterUrl)
    datasetTestClean.persist(StorageLevel.MEMORY_AND_DISK)
    
    /*Calcule IDF*/
    val tokenIdfLabel = PreProcessing.calcAndGetIdf(datasetTrainClean, num_block).collectAsMap()

    /*Class Mapping*/
    val classMapping = PreProcessing.classMapping(datasetTrainClean)

    val classNumber = classMapping.size

    /*Create formated file for MLlib*/
    val mlTrainFormated = datasetTrainClean.map { entity =>
      var sb: StringBuilder = new StringBuilder
      sb.append(classMapping(entity._1.split(":")(0)).toString).append(" ")
      var wasToken: HashSet[String] = new HashSet[String]()
      for (token <- entity._2) {
        if (wasToken.add(token)) {
          val idfLabel = tokenIdfLabel(token)
          sb.append(idfLabel._2.toString).append(":").append(1).append(" ")
        }
      }
      sb.toString.trim
    }
    var indexCount = tokenIdfLabel.size

    val mlTestFormated = datasetTestClean.map { entity =>
      if (!classMapping.contains(entity._1.split(":")(0))) {
        println("\n\nNot contains:")
        println(entity._1.split(":")(0))
        "#$@"
      } else {
        var sb: StringBuilder = new StringBuilder
        sb.append(classMapping(entity._1.split(":")(0)).toString).append(" ")
        var wasToken: HashSet[String] = new HashSet[String]()
        for (token <- entity._2) {
          if (wasToken.add(token)) {
            if (tokenIdfLabel.contains(token)) {
              val idfLabel = tokenIdfLabel(token)
              sb.append(idfLabel._2.toString).append(":").append(1).append(" ")
            }
          }
        }
        sb.toString.trim
      }
    }.filter {!_.equalsIgnoreCase("#$@")}
    
    logSb.append("Token idf size: "+tokenIdfLabel.size).append("\n")
    
    mlTrainFormated.saveAsTextFile(trainFormatedDir)
    mlTestFormated.saveAsTextFile(testFormatedDir)
    preTime = System.currentTimeMillis() - preTime
    
    MrUtils.mergeOutputFiles(trainFormatedDir, trainFormatedDir+"/train")
    MrUtils.mergeOutputFiles(testFormatedDir, testFormatedDir+"/test")
    
    runMLlibAlgorithms(logSb, classNumber, tokenIdfLabel.size, sc)
    
    sc.stop();

    logSb.append("\n"+("-"*10)+" Execution Time "+("-"*10)+"\n")
    logSb.append("PREP=").append(preTime/1000.0).append("\n")
//    logSb.append("RF=").append((preTime+rfTime)/1000.0).append("\n")
    logSb.append("NB=").append((preTime+nbTime)/1000.0).append("\n")
    logSb.append("\n" + ("*" * 40) + "\n\n").append("\n")
    println(logSb.toString)
    writeTxtLogInLocal(logSb.toString)
  }
  
  def runMLlibAlgorithms(logSb:StringBuilder, classNumber:Int, featureNumber:Int, sc:SparkContext){
    /*Run random forest*/
//    evaluationFile += "/evaluation/spark-mllib-rf-"+dataset+"-"+num_block+"-"+fold+".log"
//    rfTime = System.currentTimeMillis()
//    val logRf = RandomForestRun.run(trainFormatedDir + "/train", testFormatedDir + "/test", classNumber, featureNumber, sc)
//    rfTime = System.currentTimeMillis()-rfTime
//    logSb.append(logRf)

    /*Run naive bayes*/
    evaluationFile += "/evaluation/spark-mllib-nb-"+dataset+"-"+num_block+"-"+fold+".log"
    nbTime = System.currentTimeMillis()
    val logNb = NaiveBayesRun.runWithFeatureSelection(trainFormatedDir + "/train", testFormatedDir + "/test", featureNumber, sc)
    nbTime = System.currentTimeMillis() - nbTime
    logSb.append(logNb)
  }
  
  def runGridSearchForAlgorithms(logSb:StringBuilder, classNumber:Int, featureNumber:Int, sc:SparkContext){
    /*Run random forest*/
    rfTime = System.currentTimeMillis()
    val logRf = RandomForestRun.runGridSearch(trainFormatedDir + "/train", classNumber, featureNumber, sc)
    rfTime = System.currentTimeMillis()-rfTime
    logSb.append(logRf)
    
     /*Run naive bayes*/
//    nbTime = System.currentTimeMillis()
//    val logNb = NaiveBayesRun.runGridSearch(trainFormatedDir + "/train", featureNumber, sc)
//    nbTime = System.currentTimeMillis() - nbTime
//    logSb.append(logNb)
  }
  
  def writeTxtLogInLocal(data:String){
    val file = new File(logDir+"/mllib/spark-mllib-nb-"+num_block+"-"+dataset+"-"+fold+".txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(data)
    bw.close()
  }
}
