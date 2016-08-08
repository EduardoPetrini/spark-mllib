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
import scala.collection.mutable.ArrayBuffer

object MainSpark {

  private var sparkUrl: String = ""
  var clusterUrl: String = ""
  private var user: String = ""
  var num_block: Int = 1
  private var dataset: String = ""
  private var nbTime = 0.0
  var logDir = "/home/hadoop/petrini";//Dir for time and evaluation logs files
//  var logDir = "/home/hdp/petrini";//Dir for time and evaluation logs files
  var evaluationFile = logDir
  var lambda = 1.0
  var featurePercent = 1.0

  def printConfig() {
    println("\n" + ("*" * 40) + "\n")
    println("----MLlib Naive Bayes Cross Validation----")
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
      val fileName = Array[String]("")
      for (file <- fileName) { println("Deleting..." + file); hdfs.delete(new org.apache.hadoop.fs.Path(file), true) }
    } catch { case _: Throwable => {} }
  }

  def initialConfig(args: Array[String]) {
    println("Print args:")
    for (elem <- args) println(elem)

    if (args.length != 5) {
      println("Error, missing arguments: <num_blocks> <dataset> <master-name> <lambda> <featuresPercent>")
      System.exit(1)
    } else {
      sparkUrl = "spark://" + args(2) + ":7077"
      clusterUrl = "hdfs://" + args(2) + "/"
      num_block = args(0).toInt
      dataset = args(1)
      user = clusterUrl + "user/hdp/"
      lambda = args(3).toDouble
      featurePercent = args(4).toDouble
    }
//    excludeUsedDirs()
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
    val conf = new SparkConf().setAppName("Naive Bayes Cross Validation for "+dataset).setMaster(sparkUrl);
    val sc = new SparkContext(conf)
    var totalTime = System.currentTimeMillis()
    var sumPre = 0.0
    var sumRec = 0.0
    var sumMac = 0.0
    var sumMic = 0.0
    
    for(fold <- 0 to 9){
      val trainInputDir = user + "input/treino"+fold+".fs"
      val testInputDir = user + "input/teste"+fold+".fs"
      
      val eval = NaiveBayesRun.run(trainInputDir, testInputDir, fold, sc)
      logSb.append(eval._1)
      
      sumPre += eval._2(0).toDouble
      sumRec += eval._2(1).toDouble
      sumMac += eval._2(2).toDouble
      sumMic += eval._2(3).toDouble
    }
    totalTime = (System.currentTimeMillis() - totalTime)
    
    sc.stop();
    
    sumPre = sumPre/10
    sumRec = sumRec/10
    sumMac = sumMac/10
    sumMic = sumMic/10

    logSb.append("\n"+("-"*10)+" Execution Time "+("-"*10)+"\n")
    logSb.append("TOTAL TIME=").append(totalTime/1000.0).append("\n")
    logSb.append("\n"+("-"*10)+" Execution Evaluation "+("-"*10)+"\n")
    logSb.append("Precision\tRecall\tMacroF1\tMicroF1\n");
    logSb
    .append((f"$sumPre%.2f").toString.replace(".",",")).append("\t")
		.append((f"$sumRec%.2f").toString.replace(".",",")).append("\t")
		.append((f"$sumMac%.2f").toString.replace(".",",")).append("\t")
		.append((f"$sumMic%.2f").toString.replace(".",",")).append("\t\n\n");
//    logSb.append("RF=").append((preTime+rfTime)/1000.0).append("\n")
    logSb.append("\n" + ("*" * 40) + "\n\n").append("\n")
    println(logSb.toString)
    writeTxtLogInLocal(logSb.toString)
  }
  
  def writeTxtLogInLocal(data:String){
    val file = new File(logDir+"/mllib/spark-mllib-nb-"+num_block+"-"+dataset+"-FS:"+featurePercent+"-lb:"+lambda+".txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(data)
    bw.close()
  }
}
