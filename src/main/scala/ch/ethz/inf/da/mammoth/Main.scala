package ch.ethz.inf.da.mammoth

import breeze.linalg.SparseVector
import ch.ethz.inf.da.mammoth.io.{DatasetReader, DictionaryIO}
import ch.ethz.inf.da.mammoth.topicmodeling.{TopicModel, DistributedTopicModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.Dictionary

/**
 * Defines the command line options
 */
case class Config(
  datasetLocation: String = "",
  dictionaryLocation: String = "",
  initialModel: String = "",
  finalModel: String = "",
  seed: Int = 42,
  var topics: Int = 30,
  var vocabularySize: Int = 60000,
  globalIterations: Int = 20,
  localIterations: Int = 5,
  partitions: Int = 8192
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
      } text s"The dictionary file (if it does not exist, a dictionary will be created there)"

      opt[String]('i', "Initial model") action {
        (x, c) => c.copy(initialModel = x)
      } text s"The file containing the topic model to initialize with (leave empty to start from a random topic model)"

      opt[String]('f', "Final") action {
        (x, c) => c.copy(finalModel = x)
      } text s"The file where the final topic model will be stored"

      opt[Int]('s', "Seed") action {
        (x, c) => c.copy(seed = x)
      } text s"The random seed to initialize the topic model with (ignored when an initial model is loaded, default: ${default.seed})"

      opt[Int]('t', "topics") action {
        (x, c) => c.copy(topics = x)
      } text s"The number of topics (ignored when an initial model is loaded, default: ${default.topics})"

      opt[Int]('v', "vocabulary") action {
        (x, c) => c.copy(vocabularySize = x)
      } text s"The (maximum) size of the vocabulary (ignored when an initial model is loaded, default: ${default.vocabularySize})"

      opt[Int]('g', "globalIterations") action {
        (x, c) => c.copy(globalIterations = x)
      } text s"The number of global iterations (default: ${default.globalIterations})"

      opt[Int]('l', "localIterations") action {
        (x, c) => c.copy(localIterations = x)
      } text s"The number of local iterations (default: ${default.localIterations})"

      opt[Int]('p', "partitions") action {
        (x, c) => c.copy(partitions = x)
      } text s"The number of partitions to split the data in (default: ${default.partitions})"

    }

    parser.parse(args, Config()) foreach run

  }

  /**
   * Runs the application with provided configuration options
   *
   * @param config The command-line arguments as a configuration
   */
  def run(config: Config) {

    // Set up spark context
    val sc = createSparkContext()

    // Load the initial model or construct one depending on the command-line arguments:
    val initialModel = config.initialModel match {
      case x if x != "" => TopicModel.read(x)
      case _ => TopicModel.random(config.vocabularySize, config.topics, config.seed)
    }

    // Overwrite number of topics and vocabulary size in case an initial model was loaded from disk
    config.vocabularySize = initialModel.features
    config.topics = initialModel.topics

    // Get an RDD of all cleaned preprocessed documents
    val documents = DatasetReader.getDocuments(sc, config.datasetLocation, config.partitions)

    // Read or compute dictionary
    // We broadcast the dictionary to the spark nodes to prevent unnecessary network traffic
    val dictionary = sc.broadcast(config.dictionaryLocation match {

      // If the provided dictionary file exist, read it from disk
      case x if new java.io.File(x).exists => DictionaryIO.read(x, config.vocabularySize)

      // If the dictionary file name was provided, but the file does not exist, compute the dictionary and store it
      case x if x != "" => DictionaryIO.write(x, new Dictionary(config.vocabularySize).fit(documents))

      // No dictionary file name was provided, compute the dictionary
      case _ => new Dictionary(config.vocabularySize).fit(documents)

    })

    // Create an RDD of sparse document vectors
    val tfVectors = dictionary.value.transform(documents)

    // Convert the document vectors to breeze format
    val input = tfVectors.map(v => v.asInstanceOf[org.apache.spark.mllib.linalg.SparseVector]).
                          map(v => new SparseVector[Double](v.indices, v.values, v.size))

    // Construct distributed topic model
    val lda = new DistributedTopicModel(features         = dictionary.value.numFeatures,
                                        topics           = config.topics,
                                        globalIterations = config.globalIterations,
                                        localIterations  = config.localIterations)

    // Fit the topic model to the data
    val model = lda.fit(input, dictionary.value, initialModel)

    // Print topics of the trained topic model
    println("Final found topics")
    model.printTopics(dictionary.value, 25)

    // Save the topic model to disk if a file name was provided
    if (config.finalModel != "") {
      TopicModel.write(model, config.finalModel)
    }

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

