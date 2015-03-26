package ch.ethz.inf.da.mammoth

import ch.ethz.inf.da.mammoth.warc.WARCProcessor
import ch.ethz.inf.da.mammoth.preprocess.{htmlToText, tokenize}

/**
 * Preprocesses the raw HTML data
 */
object Preprocess {

  /**
   * Main entry point of the application
   * @param args The command-line arguments
   */
  def main(args: Array[String]) {

    // Set up spark and location of files/folders
    val input = "hdfs://localhost:9000/cw-data/*"
    val sc = Spark.createContext()

    // Distribute all WARC files
    val files = sc.wholeTextFiles(input)

    // Flat map each WARC file to multiple documents
    val htmlDocuments = files.flatMap(x => WARCProcessor.split(x._2))

    // Map each HTML document to its plain text equivalent
    val plainTextDocuments = htmlDocuments.map(x => (x._1, htmlToText(x._2)))

    // Tokenize the plain text documents
    val tokenizedDocuments = plainTextDocuments.map(x => (x._1, tokenize(x._2)))





    // Print the result
    for((id:String, tokens:Array[String]) <- tokenizedDocuments.collect()) {
      println(id)
      println(tokens.mkString(", "))
    }

  }

}

