package ch.ethz.inf.da.mammoth.warc

import java.io.{ByteArrayInputStream, File}
import de.l3s.boilerpipe.extractors.ArticleExtractor
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.jwat.warc.{WarcReaderFactory, WarcRecord}

import scala.collection.JavaConversions

/**
 * Processes WARC files
 */
object WarcProcessor {

  /**
   * Splits a WARC file into an iterator of strings representing the individual HTML documents
   *
   * @param file The WARC file to process
   * @return A lazy iterator of strings representing the HTML documents
   */
  def split(file:String, contents:String): Iterator[(String, String)] = {

    // Construct a WARC reader for the file contents
    val reader = WarcReaderFactory.getReader(new ByteArrayInputStream(contents.getBytes("UTF-8")))

    // Convert it to a scala iterator
    val iterator = JavaConversions.asScalaIterator(reader.iterator())

    // Yield each WARC record in a lazy way
    for (record: WarcRecord <- iterator; if record.getHeader("WARC-Type").value == "response") yield {
      val id = record.getHeader("WARC-TREC-ID").value
      val html = IOUtils.toString(record.getPayloadContent, "UTF-8")
      (id, html)
    }
  }

}
