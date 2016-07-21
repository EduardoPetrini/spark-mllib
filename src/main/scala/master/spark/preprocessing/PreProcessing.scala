package main.scala.master.spark.preprocessing

import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import main.scala.master.spark.util.StopWordsUtil
import org.apache.hadoop.conf.Configuration
import main.java.com.mestrado.utils.{ PreProcessingInstancias => pp }
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.Accumulator

object PreProcessing {

  def preProcess(fileName: String, stopWordFileName: String, num_block: Int, sc: SparkContext, clusterUrl: String): RDD[(String, ArrayBuffer[String])] = {
    val inputFileRDD = sc.textFile(fileName, num_block)
    val c: Configuration = new Configuration()
    c.set("fs.defaultFS", clusterUrl);

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
            for (ttSp <- tSp) newTokens += ttSp.trim
          }
        }
        
        newTokens = newTokens.sortWith(_.compareTo(_) < 0)
        
        (lineSpt(0).trim.replaceAll("\\p{P}","").replaceAll(" +","").trim.toLowerCase() + ":" + lineSpt(1).trim, newTokens)
      } else {
        (null, null)
      }
    }
  }

  def getDescription(line: Array[String]): String = {
    var desc: String = ""
    for (i <- 2 until line.length) {
      if (i == line.length - 1) desc += line(i)
      else line(i) + ";"
    }
    return desc
  }

  def calcAndGetIdf(datasetClean: RDD[(String, ArrayBuffer[String])], num_blocks:Int): RDD[(String, (Double, Int))] = {
    val totalTransaction = datasetClean.count
    var tokenCount = datasetClean flatMap { t =>
      for (token <- t._2) yield (token, 1)
    } mapValues {_ => 1L} reduceByKey(_ + _)
    tokenCount = tokenCount.sortByKey(true, 1)
    var accumulator = 0
    return tokenCount.map {
      case (token, count) =>
        accumulator += 1
        (token.trim, (calcIdf(totalTransaction, count), accumulator))
    }
    //    return null
  }

  def calcIdf(n: Double, ni: Double): Double = {
    return log2(n / ni)
  }

  def log2(num: Double): Double = {
    math.log(num) / math.log(2)
  }

  def classMapping(datasetClean: RDD[(String, ArrayBuffer[String])]): scala.collection.Map[String, Int] = {
    val classes = datasetClean.map { classOfertaId =>
      classOfertaId._1.split(":")(0)
    }.distinct
    var accumulator = -1
    return classes.map { x => 
      accumulator += 1
      (x.trim,accumulator)
    }.collectAsMap
  }
  
  def countInstancesSize(datasetClean: RDD[(String, ArrayBuffer[String])]){
    val list = datasetClean.collect()
    var big = 0
    var sma = 99999;
    var sum = 0.0
    for(i <- list){
      val size = i._2.size
      
      big = if(size > big) size; else big;
      sma = if(size < sma) size; else sma;
      sum += size
    }
    
    println("\nSmall: "+sma)
    println("Big: "+big)
    println("Average: "+(sum/list.size.toDouble)+"\n\n")
    println("Sum: "+sum)
    println("Size: "+list.size)
  }
}
