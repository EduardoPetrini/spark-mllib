package main.scala.master.spark
import org.apache.spark.{SparkConf, SparkContext}

object MainSpark{
  
  private var sparkUrl: String = ""
  private var user: String = ""
  
  def printConfig(){
   println("\n"+("*"*(sparkUrl.length+10))+"\n")
   printf("Spark url: %s\n",sparkUrl)
   printf("User dir: %s\n",user)
   println("\n"+("*"*(sparkUrl.length+10))+"\n")
  }
  
  def initialConfig(args: Array[String]){
    println("Print args:")
    for(elem <- args) println(elem)
    
    if(args.length != 1){
      println("Error, missing arguments: <master-name>")
      System.exit(1)
    }else{
      sparkUrl = "spark://"+args(0)+":7077"
      user = "/user/hdp/"
    }
    
    printConfig()
  }
  
  def main(args: Array[String]): Unit = {
    initialConfig(args)
    test()
  }
  
  
  def test() {
    val conf = new SparkConf().setAppName("First test").setMaster(sparkUrl);
    val sc = new SparkContext(conf)
        
    val inputFile = sc.textFile(user+"input/teste/teste2")
    inputFile.foreach(t => println(t))
  }
}
