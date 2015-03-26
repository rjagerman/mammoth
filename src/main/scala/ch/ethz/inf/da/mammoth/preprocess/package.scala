package ch.ethz.inf.da.mammoth

import de.l3s.boilerpipe.extractors.ArticleExtractor

/**
 * Preprocessing functions
 */
package object preprocess {

  /**
   * Processes an input stream and extracts the plain text
   *
   * @param input The input html
   * @return The plain text representation
   */
  def htmlToText(input: String): String = ArticleExtractor.INSTANCE.getText(input)

  /**
   * Tokenizes a string
   *
   * @param input The input text
   * @return The tokens
   */
  def tokenize(input: String): Array[String] = input.split("""\.+[\s$]|[/\,\+\-\!\?\:\(\)\[\]\s]+""").filter(!_.isEmpty)

}
