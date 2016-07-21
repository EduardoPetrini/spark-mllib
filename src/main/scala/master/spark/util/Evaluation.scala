package main.scala.master.spark.util

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import main.java.com.mestrado.utils.EvaluationPrediction

object Evaluation {
  def startEvaluation(predicted:RDD[Double], original:RDD[LabeledPoint]){
   var predictedArray = new java.util.ArrayList[java.lang.Double]()
   var originalArray = new java.util.ArrayList[java.lang.Double]()
   
   for(p <- predicted.collect){
     predictedArray.add(p)
   }
   for(o <- original.collect){
     originalArray.add(o.label)
   }
   
   EvaluationPrediction.startEvaluation(predictedArray, originalArray)
   
  }
}