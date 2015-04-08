package ch.ethz.inf.da.mammoth

import ch.ethz.inf.da.mammoth.feature.DictionaryTF
import ch.ethz.inf.da.mammoth.warc.{WARCProcessor, Document, TextDocument, TokenDocument}
import ch.ethz.inf.da.mammoth.preprocess.{htmlToText, tokenize, lowercase, removeStopwords, removeLessThan, removeGreaterThan}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Preprocesses the raw HTML data
 */
object Main {

  /**
   * Main entry point of the application
   * @param args The command-line arguments
   */
  def main(args: Array[String]) {

    // Set up spark context
    val sc = Spark.createContext()

    // Get an RDD of all cleaned preprocessed documents
    val documents = getDocuments(sc, "hdfs://127.0.0.1:9000/cw-data/*")

    // Compute a dictionary with a maximum size. It takes the n most frequent terms
    val dictionary = new DictionaryTF(1000)
    dictionary.fit(documents)

    // Print the dictionary
    for(word <- dictionary.mapping) {
      println(word)
    }

    // Print the document vectors as transformed by the dictionary
    for(vector <- dictionary.transform(documents).collect()) {
      println(vector)
    }

  }

  /**
   * Gets cleaned, preprocessed and tokenized documents from given input
   *
   * @param sc The spark context
   * @param input The input as an URL (e.g. hdfs://...)
   * @return An RDD of the cleaned documents
   */
  def getDocuments(sc:SparkContext, input:String): RDD[TokenDocument] = {
    // Distribute all WARC files
    val files = sc.wholeTextFiles(input)

    // Flat map each WARC file to multiple HTML documents
    val htmlDocuments = files.flatMap(x ⇒ WARCProcessor.split(x._2))

    // Map each HTML document to its plain text equivalent
    val plainTextDocuments = htmlDocuments.map(doc ⇒ new TextDocument(doc.id, htmlToText(doc.contents)))

    // Tokenize the plain text documents
    val tokenizedDocuments = plainTextDocuments.map(doc ⇒ new TokenDocument(doc.id, tokenize(doc.contents)))

    // Perform text preprocessing on the tokens
    // Convert to lowercase, remove stopwords, remove very small and very large words:
    def textProcess(tokens:Iterable[String]): Iterable[String] =
      removeGreaterThan(removeLessThan(removeStopwords(lowercase(tokens)), 2), 30)

    tokenizedDocuments.map(doc ⇒ new TokenDocument(doc.id, textProcess(doc.tokens)))

  }

}

