package main.scala.master.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.RandomForest
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.JavaConverters
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import main.scala.master.spark.main.MainSpark
import main.java.com.mestrado.utils.EvaluationPrediction
import main.scala.master.spark.util.Evaluation
import org.apache.spark.storage.StorageLevel

object RandomForestRun {

  def run(trainFileName: String, testFileName: String, numClasses: Int, featureNumber: Int, sc: SparkContext): String = {
    val timeTrainBegin = System.currentTimeMillis()
    val dataTrain = MLUtils.loadLibSVMFile(sc, trainFileName, featureNumber, MainSpark.num_block)
    dataTrain.persist(StorageLevel.MEMORY_AND_DISK)
    val treeStrategy = Strategy.defaultStrategy("Classification")

    /*If "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1 (forest) set to "sqrt".*/
    val featureSubsetStrategy = "sqrt"
    val categoricalFeaturesInfo = for (i <- 0 to featureNumber - 1) yield (i -> 2)
    var logSb: StringBuilder = new StringBuilder()

    val numTrees = 200
    val impurity = "gini"

    val model = RandomForest.trainClassifier(dataTrain, numClasses, categoricalFeaturesInfo.toMap, numTrees, featureSubsetStrategy, impurity, MainSpark.maxDepth, MainSpark.maxBins)

    val timeTrainEnd = System.currentTimeMillis()
    val timeTestBegin = System.currentTimeMillis()
    val dataTest = MLUtils.loadLibSVMFile(sc, testFileName, featureNumber, MainSpark.num_block)
    dataTest.persist(StorageLevel.MEMORY_AND_DISK)
    val predicteds = dataTest.map { point =>
      model.predict(point.features)
    }
    predicteds.cache()
    val timeTestEnd = System.currentTimeMillis()
    logSb.append("\n\n" + ("*" * 40) + "\n\n")
    logSb.append("\t--- Random Forest summary--- \n\n")
    logSb.append("\nNumTree = " + numTrees)
    logSb.append("\nMaxDepth = " + MainSpark.maxDepth)
    logSb.append("\nMaxBins = " + MainSpark.maxBins)
    logSb.append("\nImpurity = " + impurity)
    logSb.append("\nTREINO=" + (timeTrainEnd-timeTrainBegin)/1000.0)
    logSb.append("\nTESTE=" + (timeTestEnd-timeTestBegin)/1000.0)
    logSb.append("\n\n" + ("*" * 40) + "\n\n")
    
    /*Evaluation*/
    Evaluation.startEvaluation(predicteds, dataTest)
    logSb.toString
  }

  def runGridSearch(trainFileName: String, numClasses: Int, featureNumber: Int, sc: SparkContext): String = {
    val timeIni = System.currentTimeMillis()
    var logSb: StringBuilder = new StringBuilder()
    logSb.append("\n" + ("*" * 50) + "\n").append("\tRandomForest summary\n\n")

    val dataset = MLUtils.loadLibSVMFile(sc, trainFileName, featureNumber, MainSpark.num_block)
    dataset.cache()
    val tenFold = MLUtils.kFold(dataset, 10, 1)

    val treeStrategy = Strategy.defaultStrategy("Classification")
    val featureSubsetStrategy = "sqrt"
    val categoricalFeaturesInfo = for (i <- 0 to featureNumber - 1) yield (i -> 2)
    val numTrees = 200
    val impurity = "gini"

    var majorAcc = 0.0;
    var par: Map[String, String] = new HashMap[String, String]()
    var bestMaxDepth = 0
    var defaultMaxBins = 2;
    var bestMaxBins = 0;

    var accSum: Double = 0.0
    var accMean: Double = 0.0
    var index: Int = 0;

    /*Find best depth*/
    for (maxDepth <- 30 to 30) {
      accSum = 0.0;
      for (fold <- tenFold) {

        val model = RandomForest.trainClassifier(fold._1, numClasses, categoricalFeaturesInfo.toMap, numTrees, featureSubsetStrategy, impurity, maxDepth, defaultMaxBins)

        val predicteds = fold._2.map { point =>
          val prediction = model.predict(point.features)
          if (point.label == prediction) 1.0 else 0.0
        }
        predicteds.cache()
        accSum += predicteds.mean()
        println("\n\nPRINTING 1... ");
        println("acc: " + predicteds.mean());
        println("current maxDepth: " + maxDepth.toString)
        println("Fold: " + fold)
      }
      accMean = accSum / tenFold.size
      if (accMean > majorAcc) {
        majorAcc = accMean;
        par += ("maxDepth" -> maxDepth.toString)
        var localSb: StringBuilder = new StringBuilder()
        localSb.append("acc: ").append(majorAcc).append("\n par: ").append(par).append("\n")
        index += 1
        println("\n\nPRINTING 2... ");
        println("best acc: " + majorAcc);
        println("Best maxDepth: " + maxDepth.toString)
        writeTxtLogInLocal(localSb.toString, index)
      }
    }
    bestMaxDepth = par("maxDepth").toInt
    logSb.append("Finish search for maxDepth!\n")
    logSb.append("Best value for maxDepth: " + bestMaxDepth).append("\n")
    logSb.append("Best accuracy: " + majorAcc).append("\n\n")

    var localSb: StringBuilder = new StringBuilder()
    localSb.append("BEST acc: ").append(majorAcc).append("\n maxDepth: ").append(bestMaxDepth).append("\n")
    index += 1
    writeTxtLogInLocal(localSb.toString, index)

    majorAcc = 0.0
    /*Find best maxBins*/
    for (maxBins <- 12 to 12) {
      accSum = 0.0;
      for (fold <- tenFold) {

        val model = RandomForest.trainClassifier(fold._1, numClasses, categoricalFeaturesInfo.toMap, numTrees, featureSubsetStrategy, impurity, bestMaxDepth, maxBins)

        val predicteds = fold._2.map { point =>
          val prediction = model.predict(point.features)
          if (point.label == prediction) 1.0 else 0.0
        }
        predicteds.cache()
        accSum += predicteds.mean()

        println("\n\nPRINTING 3... ");
        println("acc: " + predicteds.mean());
        println("current maxBins: " + maxBins.toString)
        println("Fold: " + fold)
      }
      accMean = accSum / tenFold.size
      if (accMean > majorAcc) {
        majorAcc = accMean;
        par += ("maxBins" -> maxBins.toString)
        var localSb: StringBuilder = new StringBuilder()
        localSb.append("acc: ").append(majorAcc).append("\n par: ").append(par).append("\n")
        index += 1
        println("\n\nPRINTING 4... ");
        println("best acc: " + majorAcc);
        println("Best maxBins: " + maxBins.toString)
        writeTxtLogInLocal(localSb.toString, index)
      }
    }
    bestMaxBins = par("maxBins").toInt
    logSb.append("Finish search for maxBins!\n")
    logSb.append("Best value for maxBins: " + bestMaxBins).append("\n")
    logSb.append("Best accuracy: " + majorAcc).append("\n\n")

    localSb = new StringBuilder()
    localSb.append("BEST acc: ").append(majorAcc).append("\n maxBins: ").append(bestMaxBins).append("\n")
    index += 1
    writeTxtLogInLocal(localSb.toString, index)
    println("\n\n\nLOG\n\n" + logSb.toString)

    logSb.toString
  }

  def writeTxtLogInLocal(data: String, index: Integer) {
    val file = new File(MainSpark.logDir+"/mllib/spark-mllib-local-randomforest-log+" + index + ".log")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(data)
    bw.close()
  }
}