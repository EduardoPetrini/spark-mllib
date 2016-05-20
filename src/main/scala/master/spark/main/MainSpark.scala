package main.scala.master.spark.main
import org.apache.spark.{ SparkConf, SparkContext }
import main.scala.master.spark.preprocessing.PreProcessing
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.hadoop.fs.Path

object MainSpark {

  private var sparkUrl: String = ""
  private var clusterUrl: String = ""
  private var user: String = ""
  private var num_block: Int = 1
  private var trainFormatedDir: String = ""

  def printConfig() {
    println("\n" + ("*" * (sparkUrl.length + 10)) + "\n")
    printf("Spark url: %s\n", sparkUrl)
    printf("User dir: %s\n", user)
    println("\n" + ("*" * (sparkUrl.length + 10)) + "\n")
  }

  def excludeUsedDirs(){
    println("\n\nExclude used files...")
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(clusterUrl), hadoopConf)
    try { 
      val fileName = Array[String](trainFormatedDir)
      for(file <- fileName) {println("Deleting..."+file); hdfs.delete(new org.apache.hadoop.fs.Path(file), true)}
     } catch { case _ : Throwable => { } }
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
      user = clusterUrl + "user/hdp/"
      trainFormatedDir = user + "trainFormated";
    }
    excludeUsedDirs()
    printConfig()
  }

  def main(args: Array[String]): Unit = {
    initialConfig(args)
    val inputTrainFileName: String = user + "input/treino/treino2"
    val stopWordFileName: String = user + "input/stopwords.ser"
    val conf = new SparkConf().setAppName("First test").setMaster(sparkUrl);
    val sc = new SparkContext(conf)
    val datasetTrainClean = PreProcessing.preProcess(inputTrainFileName, stopWordFileName, num_block, sc, clusterUrl)
    datasetTrainClean.cache
    /*Calcule IDF*/
    val tokenIdfLabel = PreProcessing.calcAndGetIdf(datasetTrainClean)

    /*Class Mapping*/
    val classMapping = PreProcessing.classMapping(datasetTrainClean)

    //    for((k,v) <- tokenIdfLabel) println(k+":\t"+v._1+" | "+v._2)
    //    for((k,v) <- classMapping) println(k+":\t"+v)

    /*Create rdd to MLlib*/
    val mlTrainFormated = datasetTrainClean.map { entity =>
      var sb: StringBuilder = new StringBuilder
      sb.append(classMapping(entity._1.split(":")(0)).toString).append(" ")
      for (token <- entity._2) {
        val idfLabel = tokenIdfLabel(token)
        
        diminuir o tamanho do double aqui
        sb.append(idfLabel._2.toString).append(":").append(idfLabel._1.toString).append(" ")
      }
      sb.toString.trim
    }

    mlTrainFormated.saveAsTextFile(trainFormatedDir)
    val mlTrainInput = MLUtils.loadLabeledPoints(sc, trainFormatedDir)

    // Split data into training (60%) and test (40%).
    val splits = mlTrainInput.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

  }
}
