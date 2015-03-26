package ch.ethz.inf.da.mammoth.preprocess

import java.io.{InputStreamReader, InputStream}

import de.l3s.boilerpipe.extractors.ArticleExtractor

/**
 * Converts HTML to plain text
 */
object HtmlToText {

  /**
   * Processes an input stream and extracts the plain text using boilerpipe
   *
   * @param input The input stream
   * @return The plain text representation
   */
  def process(input: String): String = {
    ArticleExtractor.INSTANCE.getText(input)
  }
}
