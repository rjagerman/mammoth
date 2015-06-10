package ch.ethz.inf.da.mammoth

import breeze.linalg.SparseVector
import ch.ethz.inf.da.mammoth.io.{DatasetReader, DictionaryIO}
import ch.ethz.inf.da.mammoth.lda.DistributedLDA
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.Dictionary

/**
 * Defines the command line options
 */
case class Config(
  topics: Int = 30,
  globalIterations: Int = 20,
  localIterations: Int = 5,
  vocabularySize: Int = 60000,
  partitions: Int = 8192,
  datasetLocation: String = "",
  dictionaryLocation: String = ""
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
      } text "The directory where the dataset is located"

      opt[String]("dictionary") action {
        (x, c) => c.copy(dictionaryLocation = x)
      } text "The dictionary file (if it does not exist, a dictionary will be created there)"

      opt[Int]('t', "topics") action {
        (x, c) => c.copy(topics = x)
      } text s"The number of topics (default: ${default.topics})"

      opt[Int]('g', "globalIterations") action {
        (x, c) => c.copy(globalIterations = x)
      } text s"The number of global iterations (default: ${default.globalIterations})"

      opt[Int]('l', "localIterations") action {
        (x, c) => c.copy(localIterations = x)
      } text s"The number of local iterations (default: ${default.localIterations})"

      opt[Int]('v', "vocabulary") action {
        (x, c) => c.copy(vocabularySize = x)
      } text s"The (maximum) size of the vocabulary (default: ${default.vocabularySize})"

      opt[Int]('p', "partitions") action {
        (x, c) => c.copy(partitions = x)
      } text s"The number of partitions to split the data in (default: ${default.partitions})"

    }

    parser.parse(args, Config()) foreach run

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
    val documents = DatasetReader.getDocuments(sc, config.datasetLocation, config.partitions)

    // Read or compute dictionary (if computed, write to file if location was specified)
    // We broadcast the dictionary to the spark nodes to prevent unnecessary network traffic
    val dictionary = sc.broadcast(config.dictionaryLocation match {
      case x if new java.io.File(x).exists => DictionaryIO.read(x, config.vocabularySize)
      case x if x != "" => DictionaryIO.write(x, new Dictionary(config.vocabularySize).fit(documents))
      case _ => new Dictionary(config.vocabularySize).fit(documents)
    })

    // Compute document vectors and zip them with identifiers that are longs
    val tfVectors = dictionary.value.transform(documents)

    // Convert to sparse vectors in breeze format
    val sparseInput = tfVectors.map(v => v.asInstanceOf[org.apache.spark.mllib.linalg.SparseVector])
    val breezeInput = sparseInput.map(v => new SparseVector[Double](v.indices, v.values, v.size))

    // Construct LDA object
    val lda = new DistributedLDA(features = dictionary.value.numFeatures)
      .setTopics(config.topics)
      .setGlobalIterations(config.globalIterations)
      .setLocalIterations(config.localIterations)

    // Fit LDA model to data
    val model = lda.fit(breezeInput, dictionary.value)

    // Print topics
    println("Final found topics")
    model.printTopics(dictionary.value, 25)

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

