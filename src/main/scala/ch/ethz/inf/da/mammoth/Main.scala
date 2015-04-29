package ch.ethz.inf.da.mammoth

import ch.ethz.inf.da.mammoth.document.{TokenDocument, StringDocument}
import org.apache.commons.io.IOUtils
import ch.ethz.inf.da.mammoth.preprocess.{htmlToText, tokenize, lowercase, removeStopwords, stem, removeLessThan, removeGreaterThan}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{Dictionary}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.LDA
import org.jwat.warc.WarcRecord
import nl.surfsara.warcutils.WarcInputFormat

/**
 * Main application
 */
object Main {

  /**
   * Main entry point of the application
   * @param args The command-line arguments
   */
  def main(args: Array[String]) {

    // Options to configure
    val numTopics = 10
    val numIterations = 10
    val vocabularySize = 10000000
    val fileLocation = "hdfs://127.0.0.1:9000/cw-data/*"

    // Set up spark context
    val sc = createSparkContext()

    // Get an RDD of all cleaned preprocessed documents
    val documents = getDocuments(sc, fileLocation)

    // Compute document vectors and zip them with identifiers that are ints
    val dictionary = new Dictionary(vocabularySize).fit(documents)
    val tfVectors = dictionary.transform(documents)
    val ldaInput = documents.map(doc => doc.id.replaceAll("""[^0-9]+""", "").toLong).zip(tfVectors).cache()

    // Compute LDA with a specified number of topics and a specified number of 10 iterations
    val lda = new LDA().setK(numTopics).setMaxIterations(numIterations)
    val ldaModel = lda.run(ldaInput)

    // Print the computed model and its statistics
    val avgLogLikelihood = ldaModel.logLikelihood / documents.count()
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 15)
    val inverseDictionary = dictionary.mapping.map(_.swap)
    topicIndices.foreach { case (terms, termWeights) =>
      println("TOPIC:")
      terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"  ${inverseDictionary(term.toInt)}\t$weight")
      }
      println()
    }
    println(s"Avg Log-Likelihood: $avgLogLikelihood")
    println()
    println()


  }

  /**
   * Gets cleaned, preprocessed and tokenized documents from given input location of the WARC files
   *
   * @param sc The spark context
   * @param input The input as an URL (e.g. hdfs://...)
   * @return An RDD of the cleaned documents
   */
  def getDocuments(sc:SparkContext, input:String): RDD[TokenDocument] = {

    // Get the RDD representing individual WARC records
    val warcRecords:RDD[(LongWritable, WarcRecord)] =
      sc.newAPIHadoopFile(input, classOf[WarcInputFormat], classOf[LongWritable], classOf[WarcRecord])

    // Filter out records that are not reponses and get the HTML contents from the remaining WARC records
    val htmlDocuments = warcRecords.filter {
      record => record._2.getHeader("WARC-Type").value == "response"
    }.map {
      record =>
        val id = record._2.getHeader("WARC-TREC-ID").value
        val html = IOUtils.toString(record._2.getPayloadContent, "UTF-8")
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
   * Creates a spark context
   *
   * @return The spark context
   */
  def createSparkContext(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("Mammoth")
    sparkConf.set("local", "true")
    sparkConf.setMaster("local[*]")
    new SparkContext(sparkConf)
  }

}

