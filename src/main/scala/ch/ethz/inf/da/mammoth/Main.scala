package ch.ethz.inf.da.mammoth

import java.io.File

import akka.util.Timeout
import breeze.linalg.SparseVector
import ch.ethz.inf.da.mammoth.document.TokenDocument
import ch.ethz.inf.da.mammoth.io.{CluewebReader, DictionaryIO}
import ch.ethz.inf.da.mammoth.lda.{LDAConfig, Solver}
import ch.ethz.inf.da.mammoth.lda.mh.MHSolver
import ch.ethz.inf.da.mammoth.util.fileExists
import com.typesafe.config.ConfigFactory
import glint.Client
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.feature.{Dictionary, DictionaryTF}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/**
  * Defines the command line options
  */
case class Config(
                   datasetLocation: String = "",
                   dictionaryLocation: String = "",
                   finalModel: String = "",
                   checkpoint: String = "",
                   rddLocation: String = "",
                   seed: Int = 42,
                   var topics: Int = 30,
                   var vocabularySize: Int = 60000,
                   blockSize: Int = 1000,
                   α: Double = 0.5,
                   β: Double = 0.01,
                   iterations: Int = 100,
                   τ: Int = 1,
                   partitions: Int = 336,
                   glintConfig: File = new File("")
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
  def main(args: Array[String]) {

    val default = new Config()
    val parser = new scopt.OptionParser[Config]("") {
      head("Mammoth", "0.1")

      opt[String]('d', "dataset") required() action {
        (x, c) => c.copy(datasetLocation = x)
      } text "The directory where the dataset is located"

      opt[String]('r', "rdd") action {
        (x, c) => c.copy(rddLocation = x)
      } text s"The (optional) RDD vector data file to load (if it does not exist, it will be created based on the dataset)"

      opt[String]("dictionary") action {
        (x, c) => c.copy(dictionaryLocation = x)
      } text s"The dictionary file (if it does not exist, a dictionary will be created there)"

      opt[String]('f', "final") action {
        (x, c) => c.copy(finalModel = x)
      } text s"The file where the final topic model will be stored"

      opt[String]('w', "checkpoint") action {
        (x, c) => c.copy(checkpoint = x)
      } text s"The redundant file storage (e.g. HDFS) where checkpointed data will be stored"

      opt[File]('c', "glintConfig") action {
        (x, c) => c.copy(glintConfig = x)
      } text s"The glint configuration file"

      opt[Int]('s', "seed") action {
        (x, c) => c.copy(seed = x)
      } text s"The random seed to initialize the topic model with (ignored when an initial model is loaded, default: ${default.seed})"

      opt[Int]('t', "topics") action {
        (x, c) => c.copy(topics = x)
      } text s"The number of topics (ignored when an initial model is loaded, default: ${default.topics})"

      opt[Int]('v', "vocabulary") action {
        (x, c) => c.copy(vocabularySize = x)
      } text s"The (maximum) size of the vocabulary (ignored when an initial model is loaded, default: ${default.vocabularySize})"

      opt[Int]('b', "blocksize") action {
        (x, c) => c.copy(blockSize = x)
      } text s"The size of a block of parameters to process at a time (default: ${default.blockSize})"

      opt[Double]('α', "alpha") action {
        (x, c) => c.copy(α = x)
      } validate {
        case x => if (x > 0) success else failure("α must be larger than 0")
      } text s"The (symmetric) α prior on the topic-document distribution (default: ${default.α})"

      opt[Double]('β', "beta") action {
        (x, c) => c.copy(β = x)
      } validate {
        case x => if (x > 0) success else failure("β must be larger than 0")
      } text s"The (symmetric) β prior on the topic-word distribution (default: ${default.β})"

      opt[Int]('τ', "tau") action {
        (x, c) => c.copy(τ = x)
      } validate {
        x => if (x >= 1) success else failure("τ must be larger than or equal to 1")
      } text s"The SSP delay bound (default: ${default.τ})"

      opt[Int]('i', "iterations") action {
        (x, c) => c.copy(iterations = x)
      } text s"The number of iterations (default: ${default.iterations})"

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
  def run(config: Config): Unit = {

    // Create interfaces to spark and the parameter server
    val sc = createSparkContext()
    val gc = createGlintClient(config.glintConfig)

    // Read dictionary and dataset depending on configuration
    val (dictionary: DictionaryTF, vectors: RDD[SparseVector[Int]]) = readDictionaryAndDataset(sc, config)

    // Create solver
    //val lda = new LDA(config.α, config.β, config.globalIterations, config.topics, config.vocabularySize, config.seed)
    //val distributedGibbsSolver = new DistributedGibbsSolver(gc, lda, BCMHOptimizer(config.blockSize, 4))

    // Create LDA configuration
    val ldaConfig = new LDAConfig()
    ldaConfig.blockSize = config.blockSize
    ldaConfig.iterations = config.iterations
    ldaConfig.partitions = config.partitions
    ldaConfig.seed = config.seed
    ldaConfig.α = config.α
    ldaConfig.β = config.β
    ldaConfig.τ = config.τ
    ldaConfig.topics = config.topics
    ldaConfig.vocabularyTerms = dictionary.numFeatures

    // Fit data
    println(s"Computing LDA model (V = ${dictionary.numFeatures}, T = ${config.topics}, " +
      s"blocksize = ${config.blockSize}, τ = ${config.τ}, α = ${config.α}, β = ${config.β})")

    val topicModel = Solver.fit(gc, vectors, ldaConfig, (model, sspClock, id) => new MHSolver(model, sspClock, id))
    //val topicModel = distributedGibbsSolver.fit(vectors, config.partitions)

    // Construct timeout and execution context for final operations
    implicit val timeout = new Timeout(120 seconds)
    implicit val ec = ExecutionContext.Implicits.global

    // Print top topics
    topicModel.describe(15, dictionary)

    // Perform cleanup
    Await.result(topicModel.wordTopicCounts.destroy(), 60 seconds)
    Await.result(topicModel.topicCounts.destroy(), 60 seconds)

    /*val wlda = new WarpLDA(gc, config.seed, config.topics, config.vocabularySize, 2, config.α, config.β)
    wlda.fit(vectors)*/

    // Stop glint & spark
    gc.stop()
    sc.stop()

  }

  /**
    * Reads dictionary and dataset using specified configuration.
    * Depending on the settings in the configuration and the current files available it will attempt to load cached
    * copies (if specified) or compute the necessary components from scratch.
    *
    * @param sc The spark context
    * @param config The configuration
    * @return The dictionary transformer and corresponding RDD of sparse vectors (which are transformed documents)
    */
  def readDictionaryAndDataset(sc: SparkContext, config: Config): (DictionaryTF, RDD[SparseVector[Int]]) = {
    (config.rddLocation, config.dictionaryLocation) match {

      case (rddLocation, dictionaryLocation) if fileExists(sc, rddLocation) && fileExists(sc, dictionaryLocation) =>

        // Read dictionary and RDD directly from file
        (DictionaryIO.read(dictionaryLocation, config.vocabularySize), sc.objectFile(rddLocation, config.partitions))

      case (rddLocation, dictionaryLocation) =>
        // Load data set into an RDD
        val documents = CluewebReader.getDocuments(sc, config.datasetLocation, config.partitions)

        // Read or compute dictionary
        val dictionary = readDictionary(sc, documents, dictionaryLocation, config.vocabularySize)
        val dictionaryBroadcast = sc.broadcast(dictionary)

        // Vectorize data based on dictionary
        val vectors = vectorizeDataset(documents, dictionaryBroadcast).persist(StorageLevel.MEMORY_AND_DISK)

        // Store if the RDD parameter is set
        if (!rddLocation.isEmpty) {
          vectors.saveAsObjectFile(rddLocation)
        }

        (dictionary, vectors)
    }
  }

  /**
    * Reads or computes a dictionary
    *
    * @param sc The spark context
    * @param documents The documents
    * @param dictionaryLocation The dictionary location (if it exists)
    * @param vocabularySize The maximum number of vocabulary terms
    * @return A dictionary transformer
    */
  def readDictionary(sc: SparkContext,
                     documents: RDD[TokenDocument],
                     dictionaryLocation: String,
                     vocabularySize: Int): DictionaryTF = {

    dictionaryLocation match {

      // If the provided dictionary file exist, read it from disk
      case x if new java.io.File(x).exists => DictionaryIO.read(x, vocabularySize)

      // If the dictionary file name was provided, but the file does not exist, compute the dictionary and store it
      case x if x != "" => DictionaryIO.write(x, new Dictionary(vocabularySize).fit(documents))

      // No dictionary file name was provided, compute the dictionary
      case _ => new Dictionary(vocabularySize).fit(documents)
    }

  }

  /**
    * Converts given documents into sparse vector format
    *
    * @param documents The dataset
    * @param dictionary The dictionary
    * @return
    */
  def vectorizeDataset(documents: RDD[TokenDocument],
                       dictionary: Broadcast[DictionaryTF]): RDD[SparseVector[Int]] = {
    dictionary.value.transform(documents).
      map(v => v.asInstanceOf[org.apache.spark.mllib.linalg.SparseVector]).
      filter(v => v.numNonzeros > 0).
      map(v => new SparseVector[Int](v.indices, v.values.map(d => d.toInt), v.size))
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

  /**
    * Creates a glint client
    *
    * @return The glint client
    */
  def createGlintClient(glintConfig: File): Client = {
    Client(ConfigFactory.parseFile(glintConfig))
  }

}

