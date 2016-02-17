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
 */
object CluewebReader {

  /**
   * Gets the documents from given input
   *
   * @param sc The spark context
   * @param input The input
   * @return
   */
  def getDocuments(sc:SparkContext, input:String, partitions:Int): RDD[TokenDocument] = {

    // Get the warc records
    val warcRecords = input match {
      case url if url.startsWith("hdfs://") => getWarcRecordsFromHDFS(sc, url)
      case url => getWarcRecordsFromDirectory(sc, url, partitions)
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
    val tokenizedDocuments = plainTextDocuments.map(doc ⇒ new TokenDocument(doc.id, tokenize(doc.contents) toIterable))

    // Perform actual text preprocessing on the tokens
    tokenizedDocuments.map(doc ⇒ new TokenDocument(doc.id, preprocess(doc.tokens)))

  }

  /**
   * Gets WARC records from files in a directory
   *
   * @param sc The spark context
   * @param input The directory where the files are located
   * @return An RDD of the WARC records
   */
  def getWarcRecordsFromDirectory(sc:SparkContext, input:String, partitions:Int): RDD[WarcRecord] = {
    sc.wholeTextFiles(input, partitions).flatMap(x => getWarcRecordsFromString(x._2))
  }

  /**
   * Extracts WARC records from a file
   *
   * @param contents The contents of the WARC file as a string
   * @return An iterator of WarcRecords
   */
  def getWarcRecordsFromString(contents: String): Iterator[WarcRecord] = {
    val reader = WarcReaderFactory.getReader(new ByteArrayInputStream(contents.getBytes("UTF-8")))
    JavaConversions.asScalaIterator(reader.iterator())
  }

  /**
   * Gets WARC records from files on HDFS
   *
   * @param sc The spark context
   * @param input The location as an hdfs URL (e.g. hdfs://...)
   * @return An RDD of the WARC records
   */
  def getWarcRecordsFromHDFS(sc:SparkContext, input:String): RDD[WarcRecord] = {
    val warcRecords:RDD[(LongWritable, WarcRecord)] =
      sc.newAPIHadoopFile(input, classOf[WarcInputFormat], classOf[LongWritable], classOf[WarcRecord])
    warcRecords.map(x => x._2)
  }

}
