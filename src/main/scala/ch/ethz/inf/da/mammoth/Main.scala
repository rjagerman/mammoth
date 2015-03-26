package ch.ethz.inf.da.mammoth

import org.apache.spark.{SparkConf, SparkContext}
import ch.ethz.inf.da.mammoth.warc.WarcProcessor
import ch.ethz.inf.da.mammoth.preprocess.HtmlToText

/**
 * The main class
 */
object Main {
  
  /**
   * Main entry point of the application
   */
  def main(args: Array[String]) {

    // Set up spark and location of files/folders
    val input = "hdfs://localhost:9000/cw-data/*"
    val sparkConf = new SparkConf().setAppName("Mammoth")
    sparkConf.set("local", "true")
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // Distribute all WARC files
    val files = sc.wholeTextFiles(input)

    // Flat map each WARC file to multiple documents
    val htmlDocuments = files.flatMap(x => WarcProcessor.split(x._1, x._2))

    // Map each HTML document to its plain text equivalent
    val plainTextDocuments = htmlDocuments.map(x => (x._1, HtmlToText.process(x._2)))







    // Print the result
    for((id:String, text:String) <- plainTextDocuments.collect()) {
      println(id)
      println(text)
    }

  }

}

