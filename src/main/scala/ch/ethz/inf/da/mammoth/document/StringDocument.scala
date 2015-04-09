package ch.ethz.inf.da.mammoth.document

/**
 * A document whose contents is stored as a string
 *
 * @param id The unique identifier
 * @param contents The contents of the document
 */
case class StringDocument(id: String, contents: String) extends Document(id)
