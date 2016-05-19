package main.scala.master.spark.main
import org.apache.spark.{ SparkConf, SparkContext }
import main.scala.master.spark.preprocessing.PreProcessing

object MainSpark {

  private var sparkUrl: String = ""
  private var clusterUrl: String = ""
  private var user: String = ""
  private var num_block: Int = 1

  def printConfig() {
    println("\n" + ("*" * (sparkUrl.length + 10)) + "\n")
    printf("Spark url: %s\n", sparkUrl)
    printf("User dir: %s\n", user)
    println("\n" + ("*" * (sparkUrl.length + 10)) + "\n")
  }

  def initialConfig(args: Array[String]) {
    println("Print args:")
    for (elem <- args) println(elem)

    if (args.length != 1) {
      println("Error, missing arguments: <master-name>")
      System.exit(1)
    } else {
      sparkUrl = "spark://" + args(0) + ":7077"
      clusterUrl = "hdfs://" + args(0) + "/"
      user = clusterUrl+"user/hdp/"
    }

    printConfig()
  }

  def main(args: Array[String]): Unit = {
    initialConfig(args)
    val inputTrainFileName: String = user+"input/treino/treino2"
    val stopWordFileName: String = user+"input/stopwords.ser"
    val conf = new SparkConf().setAppName("First test").setMaster(sparkUrl);
    val sc = new SparkContext(conf)
    
    val datasetTrainClean = PreProcessing.preProcess(inputTrainFileName, stopWordFileName, num_block, sc, clusterUrl)
    
    /*Calcule IDF*/
    val tokenIdf = PreProcessing.calcAndGetIdf(datasetTrainClean)
    
    for((k,v) <- tokenIdf) println(k+":\t"+v)
    
    /*Create rdd to MLlib*/
  }

}
