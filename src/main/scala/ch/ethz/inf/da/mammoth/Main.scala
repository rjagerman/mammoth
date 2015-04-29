package ch.ethz.inf.da.mammoth

import ch.ethz.inf.da.mammoth.io.DatasetReader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{Dictionary}
import org.apache.spark.mllib.clustering.LDA

/**
 * Defines the command line options
 */
case class Config(
  numTopics: Int = 10,
  numIterations: Int = 10,
  vocabularySize: Int = 10000000,
  datasetLocation: String = ""
)

/**
 * Main application
 */
object Main {

  /**
   * Entry point of the application
   * This parses the command line options and executes the run method
   *
   * @param args The command line arguments
   */
  def main(args:Array[String]) {

    val default = new Config()
    val parser = new scopt.OptionParser[Config]("") {
      head("Mammoth", "0.1")

      opt[String]('d', "dataset") required() action {
        (x, c) => c.copy(datasetLocation = x)
      } text("The directory where the dataset is located")

      opt[Int]('t', "topics") action {
        (x, c) => c.copy(numTopics = x)
      } text(s"The number of topics (default: ${default.numTopics})")

      opt[Int]('i', "iterations") action {
        (x, c) => c.copy(numIterations = x)
      } text(s"The number of iterations (default: ${default.numIterations})")

      opt[Int]('v', "vocabulary") action {
        (x, c) => c.copy(vocabularySize = x)
      } text(s"The (maximum) size of the vocabulary (default: ${default.vocabularySize})")

    }

    parser.parse(args, Config()) map (run)

  }

  /**
   * Runs the topic modeling
   *
   * @param config The command-line arguments as a configuration
   */
  def run(config: Config) {

    // Set up spark context
    val sc = createSparkContext()

    // Get an RDD of all cleaned preprocessed documents
    val documents = new DatasetReader(sc).getDocuments(config.datasetLocation)

    // Compute document vectors and zip them with identifiers that are ints
    val dictionary = new Dictionary(config.vocabularySize).fit(documents)
    val tfVectors = dictionary.transform(documents)
    val ldaInput = documents.map(doc => doc.id.replaceAll("""[^0-9]+""", "").toLong).zip(tfVectors).cache()

    // Compute LDA with a specified number of topics and a specified number of 10 iterations
    val lda = new LDA().setK(config.numTopics).setMaxIterations(config.numIterations)
    val ldaModel = lda.run(ldaInput)

    // Print the computed model and its statistics
    val avgLogLikelihood = ldaModel.logLikelihood / documents.count()
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 15)
    val inverseDictionary = dictionary.mapping.map(_.swap)
    topicIndices.zipWithIndex.foreach { case ((terms, termWeights), idx) =>
      println(s"TOPIC ${idx}:")
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
   * Creates a spark context
   *
   * @return The spark context
   */
  def createSparkContext(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("Mammoth")
    new SparkContext(sparkConf)
  }

}

