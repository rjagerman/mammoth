package ch.ethz.inf.da.mammoth.io

import java.io.ByteArrayInputStream

import ch.ethz.inf.da.mammoth.document.{StringDocument, TokenDocument}
import ch.ethz.inf.da.mammoth.preprocess._
import nl.surfsara.warcutils.WarcInputFormat
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jwat.warc.{WarcReaderFactory, WarcRecord}

import scala.collection.JavaConversions

/**
 * Helper object for reading the data set
 *
 * @param sc The spark context
 */
class DatasetReader(sc:SparkContext) {

  /**
   * Gets the documents from given input
   *
   * @param input The input
   * @return
   */
  def getDocuments(input:String): RDD[TokenDocument] = {

    // Get the warc records
    val warcRecords = input match {
      case url if url.startsWith("hdfs://") => getWarcRecordsFromHDFS(url)
      case url => getWarcRecordsFromDirectory(url)
    }

    // Filter out records that are not reponses and get the HTML contents from the remaining WARC records
    val htmlDocuments = warcRecords.filter {
      record => record.getHeader("WARC-Type").value == "response"
    }.map {
      record =>
        val id = record.getHeader("WARC-TREC-ID").value
        val html = IOUtils.toString(record.getPayloadContent, "UTF-8")
        new StringDocument(id, html)
    }

    // Map each HTML document to its plain text equivalent
    val plainTextDocuments = htmlDocuments.map(doc ⇒ new StringDocument(doc.id, htmlToText(doc.contents)))

    // Tokenize the plain text documents
    val tokenizedDocuments = plainTextDocuments.map(doc ⇒ new TokenDocument(doc.id, tokenize(doc.contents)))

    // Perform text preprocessing on the tokens
    // Convert to lowercase, remove stopwords, remove very small and very large words:
    def textProcess(tokens:Iterable[String]): Iterable[String] =
      stem(removeGreaterThan(removeLessThan(removeStopwords(lowercase(tokens)), 2), 30))

    tokenizedDocuments.map(doc ⇒ new TokenDocument(doc.id, textProcess(doc.tokens)))

  }

  /**
   * Gets cleaned, preprocessed and tokenized documents from given input location of the WARC files
   *
   * @param input
   * @return
   */
  def getWarcRecordsFromDirectory(input:String): RDD[WarcRecord] = {
    this.sc.wholeTextFiles(input).flatMap(x => getWarcRecordsFromString(x._2))
  }

  /**
   * Extracts WARC records from a file
   *
   * @param contents The contents of the WARC file as a string
   * @return A lazy iterator of WarcRecords
   */
  def getWarcRecordsFromString(contents: String): Iterator[WarcRecord] = {
    val reader = WarcReaderFactory.getReader(new ByteArrayInputStream(contents.getBytes("UTF-8")))
    JavaConversions.asScalaIterator(reader.iterator())
  }

  /**
   * Extracts an RDD of all WARC records from HDFS
   *
   * @param input The input as an hdfs URL (e.g. hdfs://...)
   * @return An RDD of the cleaned documents
   */
  def getWarcRecordsFromHDFS(input:String): RDD[WarcRecord] = {
    val warcRecords:RDD[(LongWritable, WarcRecord)] =
      this.sc.newAPIHadoopFile(input, classOf[WarcInputFormat], classOf[LongWritable], classOf[WarcRecord])
    warcRecords.map(x => x._2)
  }

}
