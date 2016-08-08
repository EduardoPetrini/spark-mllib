package main.scala.master.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.SparseVector
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import main.scala.master.spark.main.MainSpark
import main.java.com.mestrado.utils.EvaluationPrediction
import main.scala.master.spark.util.Evaluation
import org.apache.spark.storage.StorageLevel

object NaiveBayesRun {
  def run(trainFileName: String, testFileName: String, fold:Integer, sc: SparkContext): (String,Array[String]) = {
    val timeTrainBegin = System.currentTimeMillis()
    val dataTrain = MLUtils.loadLabeledPoints(sc, trainFileName, MainSpark.num_block)
    dataTrain.persist(StorageLevel.MEMORY_AND_DISK)
    val model = NaiveBayes.train(dataTrain, MainSpark.lambda, modelType = "multinomial")
    val timeTrainEnd = System.currentTimeMillis()
    
    val timeTestBegin = System.currentTimeMillis()
    val dataTest = MLUtils.loadLabeledPoints(sc, testFileName, MainSpark.num_block)
    dataTest.persist(StorageLevel.MEMORY_AND_DISK)
    val predicteds = dataTest.map { point =>
      model.predict(point.features)
    }
    predicteds.cache
    val timeTestEnd = System.currentTimeMillis()
    var logSb: StringBuilder = new StringBuilder()
    logSb.append("\n\n" + ("*" * 40) + "\n\n")
    logSb.append("\t--- Naive Bayes summary for FOLD "+fold+" ---\n\n")
    logSb.append("Lambda = " + MainSpark.lambda)
    logSb.append("\nTREINO=" +(timeTrainEnd-timeTrainBegin)/1000.0)
    logSb.append("\nTESTE=" +(timeTestEnd-timeTestBegin)/1000.0)
    logSb.append("\n\nPrecision\tRecall\tMacroF1\tMicroF1\n");
    val eval = Evaluation.startEvaluation(predicteds, dataTest)
    logSb.append(eval(0)).append("\t").append(eval(1)).append("\t").append(eval(2)).append("\t").append(eval(3)).append("\n\n")
    logSb.append("\n\n" + ("*" * 40) + "\n\n")
    
    (logSb.toString, eval)
  }
   
  def writeTxtLogInLocal(data:String, index:Integer){
    val file = new File(MainSpark.logDir+"/mllib/spark-mllib-local-naivebayes-log+"+index+".log")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(data)
    bw.close()
  }
}