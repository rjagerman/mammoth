package ch.ethz.inf.da.mammoth

import java.io.ByteArrayInputStream

import ch.ethz.inf.da.mammoth.document.StringDocument
import org.apache.commons.io.IOUtils
import org.jwat.warc.{WarcRecord, WarcReaderFactory}

import scala.collection.JavaConversions

/**
 * Contains functions for parsing WARC files
 */
package object warc {

  /**
   * Splits a WARC file into an iterator of strings representing the individual HTML documents
   *
   * @param contents The contents of the WARC file as a string
   * @return A lazy iterator of strings representing the HTML documents
   */
  def splitWarcFile(contents:String): Iterator[StringDocument] = {

    // Construct a WARC reader for the file contents
    val reader = WarcReaderFactory.getReader(new ByteArrayInputStream(contents.getBytes("UTF-8")))

    // Convert it to a scala iterator
    val iterator = JavaConversions.asScalaIterator(reader.iterator())

    // Yield each WARC record in a lazy way
    for (record: WarcRecord <- iterator; if record.getHeader("WARC-Type").value == "response") yield {
      val id = record.getHeader("WARC-TREC-ID").value
      val html = IOUtils.toString(record.getPayloadContent, "UTF-8")
      new StringDocument(id, html)
    }
  }

}
