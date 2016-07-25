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
  def run(trainFileName: String, testFileName: String, featureNumber: Int, sc: SparkContext): String = {
    val timeTrainBegin = System.currentTimeMillis()
    val dataTrain = MLUtils.loadLibSVMFile(sc, trainFileName, featureNumber)
    dataTrain.persist(StorageLevel.MEMORY_AND_DISK)
    val model = NaiveBayes.train(dataTrain, MainSpark.lambda, modelType = "multinomial")
    val timeTrainEnd = System.currentTimeMillis()
    
    val timeTestBegin = System.currentTimeMillis()
    val dataTest = MLUtils.loadLibSVMFile(sc, testFileName, featureNumber)
    dataTest.persist(StorageLevel.MEMORY_AND_DISK)
    val predicteds = dataTest.map { point =>
      model.predict(point.features)
    }
    predicteds.cache
    val timeTestEnd = System.currentTimeMillis()
    var logSb: StringBuilder = new StringBuilder()
    logSb.append("\n\n" + ("*" * 40) + "\n\n")
    logSb.append("\t--- Naive Bayes summary ---\n\n")
    logSb.append("Lambda = " + MainSpark.lambda)
    logSb.append("\nTREINO=" +(timeTrainEnd-timeTrainBegin)/1000.0)
    logSb.append("\nTESTE=" +(timeTestEnd-timeTestBegin)/1000.0)
    logSb.append("\n\n" + ("*" * 40) + "\n\n")

    Evaluation.startEvaluation(predicteds, dataTest)
    
    logSb.toString
  }

  def runGridSearch(trainFileName: String, featureNumber: Int, sc: SparkContext): String = {
    val timeIni = System.currentTimeMillis()

    val dataTrain = MLUtils.loadLibSVMFile(sc, trainFileName, featureNumber, MainSpark.num_block)
    val tenFold = MLUtils.kFold(dataTrain, 10, 1)
    var accSum = 0.0;
    var accMean = 0.0;
    var bestLambda = 0.0
    var majorAcc = 0.0
    var index = 0
    
    for (lambda <- 0.0001 to 0.002 by 0.002) {
      accSum = 0.0;
      for (fold <- tenFold) {
        val dataset = NaiveBayes.train(fold._1, lambda, modelType = "multinomial")
        val predicteds = fold._2.map { point =>
          val prediction = dataset.predict(point.features)
          if (point.label == prediction) 1.0 else 0.0
        }
        predicteds.cache()
        accSum += predicteds.mean()
        println("\n\nPRINTING 1... ");
        println("acc: "+predicteds.mean());
        println("current lambda: "+lambda.toString)
        println("Fold: "+fold)
      }
      accMean = accSum/tenFold.size
      if(accMean > majorAcc){
        bestLambda = lambda
        majorAcc = accMean;
        var localSb: StringBuilder = new StringBuilder()
        localSb.append("acc: ").append(majorAcc).append("\n lambda: ").append(lambda.toString).append("\n")
        index += 1
        println("\n\nPRINTING 2... ");
        println("best acc: "+majorAcc);
        println("Best lambda: "+lambda.toString)
        writeTxtLogInLocal(localSb.toString, index)
      }
    }


    val timeEnd = System.currentTimeMillis()

    var logSb: StringBuilder = new StringBuilder()
    logSb.append("\n\n" + ("*" * 40) + "\n\n")
    logSb.append("\t--- Naive Bayes summary ---\n\n")
    logSb.append("best lambda = " + bestLambda)
    logSb.append("\nAcc = " + majorAcc)
    logSb.append("\nTime = " + ((timeEnd - timeIni) / 1000.0))
    logSb.append("\n\n" + ("*" * 40) + "\n\n")

    logSb.toString
  }
  
  def writeTxtLogInLocal(data:String, index:Integer){
    val file = new File(MainSpark.logDir+"/mllib/spark-mllib-local-naivebayes-log+"+index+".log")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(data)
    bw.close()
  }
}