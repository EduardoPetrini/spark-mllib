package main.scala.master.spark.util

import org.apache.hadoop.conf.Configuration
import java.io.ObjectInputStream
import java.io.InputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import main.java.com.mestrado.app.{ StopWords => swClass };

object StopWordsUtil {
  def readStopWordObjectFromHDFS(swFileName: String, c: Configuration): java.util.Set[String] = {
//    var sw: Set[String] = Set()
    val sw: java.util.Set[String] = swClass.readObject(swFileName, c)
    return sw
  }

//  def main(args: Array[String]): Unit = {
//    val fileName: String = "hdfs://note-home/user/hdp/input/stopwords.ser"
//    val c: Configuration = new Configuration()
//    c.set("fs.defaultFS", "hdfs://note-home/");
//    readStopWordObjectFromHDFS(fileName, c)
//  }
}
