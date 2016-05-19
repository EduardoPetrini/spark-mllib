package main.scala.master.spark.preprocessing

import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import main.scala.master.spark.util.StopWordsUtil
import org.apache.hadoop.conf.Configuration
import main.java.com.mestrado.utils.{ PreProcessingInstancias => pp }
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

object PreProcessing {

  def preProcess(fileName: String, stopWordFileName: String, num_block: Int, sc: SparkContext, clusterUrl:String) :RDD[(String,ArrayBuffer[String])] = {
    val inputFileRDD = sc.textFile(fileName, num_block)
    val c: Configuration = new Configuration()
    c.set("fs.defaultFS", "hdfs://note-home/");
    
    val stopWords: java.util.Set[String] = StopWordsUtil.readStopWordObjectFromHDFS(stopWordFileName, c)
    
    return inputFileRDD.map { line =>
      var lineSpt = line.split(";")
      if (lineSpt.length > 2) {
        lineSpt(2) = getDescription(lineSpt)
        val tokens = pp.tratarDescription(lineSpt(2)).split(" ")
        var newTokens: ArrayBuffer[String] = ArrayBuffer()
        for (t <- tokens) {
          val mt = pp.processaToken(t.trim, stopWords).asScala
          for (i <- 0 until mt.size) {
            val tSp: Array[String] = mt(i).split(" ")
            for (ttSp <- tSp) newTokens += ttSp
          }
        }
        
        (lineSpt(0).trim+":"+lineSpt(1).trim,newTokens)
      }else{
        (null,null)
      }
    }
  }
  
  def getDescription(line: Array[String]) : String = {
    var desc: String = ""
    for(i <- 2 until line.length){
      if(i == line.length-1) desc += line(i)
      else line(i)+";"
    }
    return desc
  }
  
  def calcAndGetIdf(datasetClean: RDD[(String,ArrayBuffer[String])]) : scala.collection.Map[String,Double] = {
    val totalTransaction = datasetClean.count
    val tokenCount = datasetClean flatMap {t =>
      for(token <- t._2) yield (token,1)
    } countByKey
    
   return tokenCount.map { case (token,count) =>
//      val idf = calcIdf(totalTransaction,count)
      (token, calcIdf(totalTransaction,count))
      
    }
//    return null
  }
  
  def calcIdf(n:Double, ni:Double): Double = {
    return log2(n/ni)
  }
  
  def log2(num:Double): Double = {
    math.log(num)/math.log(2)
  }
}
