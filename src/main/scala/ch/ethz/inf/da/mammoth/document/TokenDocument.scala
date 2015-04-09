package ch.ethz.inf.da.mammoth.document

/**
 * A document whose contents is stored as a list of tokens
 *
 * @param id The unique identifier
 * @param tokens The tokens representing the document
 */
case class TokenDocument(id: String, tokens: Iterable[String]) extends Document(id) with Iterable[String] {
  override def iterator: Iterator[String] = tokens.iterator
}
