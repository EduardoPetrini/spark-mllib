package main.scala.master.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import main.scala.master.spark.main.MainSpark
import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.regression.LabeledPoint

object FeaturesSelection {
  
  def selectFeatures(trainFileName:String, testFileName:String, totalFeatures:Integer, selectPercent:Double, sc:SparkContext) : (String,String) ={
    val trainOut = trainFileName+".fs"
    val testOut = testFileName+".fs"
    val filterTo = totalFeatures*selectPercent
    val dataTrain = MLUtils.loadLibSVMFile(sc, trainFileName, totalFeatures, MainSpark.num_block)
    val dataTest = MLUtils.loadLibSVMFile(sc, testFileName, totalFeatures, MainSpark.num_block)
    val selector = new ChiSqSelector(filterTo.toInt)
    val transformer = selector.fit(dataTrain)
    val filteredDataTrain = dataTrain.map { lp =>
      LabeledPoint(lp.label, transformer.transform(lp.features))
    }
    
    val filteredDataTest = dataTest.map { lp =>
      LabeledPoint(lp.label, transformer.transform(lp.features))
    }
    
    filteredDataTrain.saveAsTextFile(trainOut)
    filteredDataTest.saveAsTextFile(testOut)
    (trainOut,testOut)
  }
}