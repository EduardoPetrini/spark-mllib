package main.scala.master.spark.util

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import scala.collection.mutable.HashSet

object BuildArff {
  def createArffData(classMapping:scala.collection.Map[String,Int], tokenIdfUnsorted:scala.collection.Map[String,(Double,Int)], dataRdd:RDD[(String,ArrayBuffer[String])], datasetName:String){
	  val tokenIdf = tokenIdfUnsorted.toList.sortBy(t => t._2._2)
    var sbHeader = new StringBuilder()
    sbHeader.append("@RELATION ").append(datasetName).append("\n\n")
    for(tIdf <- tokenIdf){
      sbHeader.append("@ATTRIBUTE ").append("attr"+(tIdf._2._2-1)).append(" REAL\n")     
    }
    sbHeader.append("@ATTRIBUTE class {")
    for(cl <- classMapping){
      sbHeader.append(cl._1).append(", ")
    }
  
    sbHeader.replace(sbHeader.length-2, sbHeader.length, "}\n\n")
    sbHeader.append("@DATA\n")
    
    val classIndex = tokenIdf.size
    
    val dataRddFormated = dataRdd.map { entity =>
      var sbData: StringBuilder = new StringBuilder
      sbData.append("{")
      var wasToken: HashSet[String] = new HashSet[String]()
      for (token <- entity._2) {
        if (wasToken.add(token)) {
          if (tokenIdfUnsorted.contains(token)) {
            val idfLabel = tokenIdfUnsorted(token)
            sbData.append((idfLabel._2-1).toString).append(" ").append(idfLabel._1.toString).append(", ")
          }
        }
      }
      sbData.append(classIndex).append(" ").append(entity._1.split(":")(0)).append("}\n")
      
      sbData.toString.trim
    }.collect()
//    println(sbHeader.toString)
//    dataRddFormated.foreach { x => println(x)}
    
    val file = new File("/home/hdp/input-er/arff/"+datasetName+".arff")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(sbHeader.toString)
    dataRddFormated.foreach { x => bw.write(x+"\n")}
    bw.close()
  }
  
  def saveTxtLocalFile(fileName:String, content:String){
    val file = new File("arff/"+fileName+".arff")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(content)
    bw.close()
  }
}