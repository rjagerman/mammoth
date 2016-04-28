package ch.ethz.inf.da.mammoth

import java.io.File

import akka.util.Timeout
import breeze.linalg.{sum, SparseVector}
import ch.ethz.inf.da.mammoth.document.TokenDocument
import ch.ethz.inf.da.mammoth.io.{CluewebReader, DictionaryIO}
import glint.iterators.RowBlockIterator
import glint.models.client.buffered.BufferedBigMatrix
import glintlda.{LDAModel, LDAConfig, Solver}
import ch.ethz.inf.da.mammoth.util.fileExists
import com.typesafe.config.ConfigFactory
import glint.Client
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.feature.{Dictionary, DictionaryTF}
import org.apache.spark.mllib.clustering.{OnlineLDAOptimizer, LocalLDAModel, DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/**
  * Defines the command line options
  */
case class Config(
                   algorithm: String = "mh",
                   blockSize: Int = 1000,
                   checkpointSave: String = "",
                   checkpointRead: String = "",
                   checkpointEvery: Int = 1,
                   glintConfig: File = new File(""),
                   datasetLocation: String = "",
                   dictionaryLocation: String = "",
                   finalModel: String = "",
                   forceRepartition: Boolean = false,
                   iterations: Int = 100,
                   mhSteps: Int = 2,
                   partitions: Int = 336,
                   rddLocation: String = "",
                   seed: Int = 42,
                   subsample: Double = 1.0,
                   testSize: Double = 0.0,
                   var topics: Int = 30,
                   var vocabularySize: Int = 60000,
                   α: Double = 0.5,
                   β: Double = 0.01,
                   τ: Int = 1
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

      opt[String]('a', "algorithm") action {
        (x, c) => c.copy(algorithm = x)
      } validate {
        case x: String =>
          if (Set("sparkonline", "sparkem", "mh", "naive").contains(x)) {
            success
          } else {
            failure("algorithm must be either sparkonline, sparkem, mh or naive")
          }
      } text s"The algorithm to use (either sparkonline, sparkem, mh or naive)"

      opt[Int]('b', "blocksize") action {
        (x, c) => c.copy(blockSize = x)
      } text s"The size of a block of parameters to process at a time (default: ${default.blockSize})"

      opt[File]('c', "glintconfig") action {
        (x, c) => c.copy(glintConfig = x)
      } text s"The glint configuration file"

      opt[String]('d', "dataset") required() action {
        (x, c) => c.copy(datasetLocation = x)
      } text "The directory where the dataset is located"

      opt[String]("dictionary") action {
        (x, c) => c.copy(dictionaryLocation = x)
      } text s"The dictionary file (if it does not exist, a dictionary will be created there)"

      opt[String]('f', "final") action {
        (x, c) => c.copy(finalModel = x)
      } text s"The file where the final topic model will be stored"

      opt[Int]('i', "iterations") action {
        (x, c) => c.copy(iterations = x)
      } text s"The number of iterations (default: ${default.iterations})"

      opt[Int]('m', "metropolishastings") action {
        (x, c) => c.copy(mhSteps = x)
      }  validate {
        case x => if (x > 0) success else failure("Number of metropolis-hastings steps must be larger than 0")
      } text s"The number of metropolis-hastings steps (default: ${default.mhSteps})"

      opt[Int]('p', "partitions") action {
        (x, c) => c.copy(partitions = x)
      } text s"The number of partitions to split the data in (default: ${default.partitions})"

      opt[String]('r', "rdd") action {
        (x, c) => c.copy(rddLocation = x)
      } text s"The (optional) RDD vector data file to load (if it does not exist, it will be created based on the dataset)"

      opt[Int]('s', "seed") action {
        (x, c) => c.copy(seed = x)
      } text s"The random seed to initialize the topic model with (ignored when an initial model is loaded, default: ${default.seed})"

      opt[Int]('t', "topics") action {
        (x, c) => c.copy(topics = x)
      } text s"The number of topics (ignored when an initial model is loaded, default: ${default.topics})"

      opt[Int]('v', "vocabulary") action {
        (x, c) => c.copy(vocabularySize = x)
      } text s"The (maximum) size of the vocabulary (ignored when an initial model is loaded, default: ${default.vocabularySize})"

      opt[String]("checkpointSave") action {
        (x, c) => c.copy(checkpointSave = x)
      } text s"The location where checkpointed data will be stored after failure"

      opt[String]("checkpointRead") action {
        (x, c) => c.copy(checkpointRead = x)
      } text s"The location where checkpointed data will be read from as a warmstart mechanic"

      opt[Int]("checkpointEvery") action {
        (x, c) => c.copy(checkpointEvery = x)
      } validate {
        case x => if (x >= 1) success else failure("checkpointEvery must be larger or equal to 1")
      } text s"If checkpointSave is set, this indicates the frequency of checkpoints (every x iterations)"

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

      opt[Double]("test") action {
        (x, c) => c.copy(testSize = x)
      } validate {
        case x => if (x >= 0.0 && x <= 1.0) success else failure("test must be between in range [0.0, 1.0]")
      } text s"The fraction of the data set to use for testing instead of training"

      opt[Double]("subsample") action {
        (x, c) => c.copy(subsample = x)
      } validate {
        case x => if (x >= 0.0 && x <= 1.0) success else failure("subsample must be between in range [0.0, 1.0]")
      } text s"The subsample ratio (default: ${default.subsample})"

      opt[Boolean]("forceRepartition") action {
        (x, c) => c.copy(forceRepartition = x)
      } text s"Whether to force a repartition before training (default: ${default.forceRepartition})"

      opt[Int]('τ', "tau") action {
        (x, c) => c.copy(τ = x)
      } validate {
        x => if (x >= 1) success else failure("τ must be larger than or equal to 1")
      } text s"The SSP delay bound (default: ${default.τ})"

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
    val (dictionary: DictionaryTF, fullDataset: RDD[SparseVector[Int]]) = readDictionaryAndDataset(sc, config)

    // Sub sample if needed
    val sampledDataset = if (config.subsample < 1.0) {
      fullDataset.sample(false, config.subsample, config.seed)
    } else {
      fullDataset
    }

    // Data set size
    println(s"Data set size: ${sampledDataset.count()}")

    // Split training and test size (if required, otherwise keep original RDD for training)
    var (trainingSet: RDD[SparseVector[Int]], testSet: RDD[SparseVector[Int]]) = if (config.testSize > 0.0) {
      val randomSplit = sampledDataset.randomSplit(Array(1.0 - config.testSize, config.testSize), config.seed)
      (randomSplit(0), randomSplit(1))
    } else {
      (sampledDataset, sc.emptyRDD[SparseVector[Int]])
    }

    // Perform forced repartition if necessary
    if (config.forceRepartition) {
      trainingSet = trainingSet.repartition(config.partitions)
      testSet = testSet.repartition(config.partitions)
    }

    config.algorithm match {
      case "mh" => solve(config, dictionary, trainingSet, testSet, sc, gc, "mh")
      case "naive" => solve(config, dictionary, trainingSet, testSet, sc, gc, "naive")
      case "sparkonline" => solveSpark(config, dictionary, trainingSet, testSet, sc, gc)
      case "sparkem" => solveSpark(config, dictionary, trainingSet, testSet, sc, gc)
    }

    // Stop glint & spark
    gc.stop()
    sc.stop()

    // Exit application
    System.exit(0)

  }

  /**
    * Solves using Mammoth algorithm
    *
    * @param config The LDA configuration
    * @param dictionary The dictionary
    * @param trainingSet The data set as an RDD of sparse vectors
    * @param sc The spark context
    * @param gc The glint client
    * @param solver The solver to use
    */
  def solve(config: Config,
            dictionary: DictionaryTF,
            trainingSet: RDD[SparseVector[Int]],
            testSet: RDD[SparseVector[Int]],
            sc: SparkContext,
            gc: Client,
            solver: String): Unit = {

    // Create LDA configuration
    val ldaConfig = new LDAConfig()
    ldaConfig.setBlockSize(config.blockSize)
    ldaConfig.setIterations(config.iterations)
    ldaConfig.setMhSteps(config.mhSteps)
    ldaConfig.setPartitions(config.partitions)
    ldaConfig.setSeed(config.seed)
    ldaConfig.setTopics(config.topics)
    ldaConfig.setVocabularyTerms(dictionary.numFeatures)
    ldaConfig.setPowerlawCutoff(2000000 / config.topics)
    ldaConfig.setα(config.α)
    ldaConfig.setβ(config.β)
    ldaConfig.setτ(config.τ)
    ldaConfig.setCheckpointSave(config.checkpointSave)
    ldaConfig.setCheckpointRead(config.checkpointRead)
    ldaConfig.setCheckpointEvery(config.checkpointEvery)

    // Print LDA config
    println(s"Computing LDA model")
    println(ldaConfig)

    // Fit data to topic model
    val topicModel = solver match {
      case "mh" =>
        val startTime = System.currentTimeMillis()
        val model = Solver.fitMetropolisHastings(sc, gc, trainingSet, ldaConfig)
        val totalTime = System.currentTimeMillis() - startTime
        println(s"Total training time: ${totalTime}ms")

        if (config.testSize > 0.0) {
          Solver.test(sc, gc, model, testSet, 50)
        }
        model

      case "naive" =>
        val startTime = System.currentTimeMillis()
        val model = Solver.fitNaive(sc, gc, trainingSet, ldaConfig)
        val totalTime = System.currentTimeMillis() - startTime
        println(s"Total training time: ${totalTime}ms")

        if (config.testSize > 0.0) {
          Solver.test(sc, gc, model, testSet, 50)
        }
        model
    }

    // Construct timeout and execution context for final operations
    implicit val timeout = new Timeout(300 seconds)
    implicit val ec = ExecutionContext.Implicits.global

    // Print top 50 topics
    var topicNumber = 1
    val inverseMap = dictionary.mapping.map(_.swap)
    topicModel.describe(50).foreach {
      case topicDescription =>
        println(s"Topic $topicNumber")
        topicNumber += 1
        for (i <- 0 until topicDescription.length) {
          val k = topicDescription(i)._1.toInt
          val p = topicDescription(i)._2
          println(s"   ${inverseMap(k)}:         $p")
        }
    }

    // Write to file
    if (!config.finalModel.isEmpty) {
      topicModel.writeProbabilitiesToCSV(new File(config.finalModel))
    }

    // Perform cleanup
    Await.result(topicModel.wordTopicCounts.destroy(), timeout.duration)
    Await.result(topicModel.topicCounts.destroy(), timeout.duration)

  }

  /**
    * Solves using Spark algorithm
    *
    * @param config The LDA configuration
    * @param dictionary The dictionary
    * @param trainingSet The data set as an RDD of sparse vectors
    * @param sc The spark context
    * @param gc The glint client
    */
  def solveSpark(config: Config,
                 dictionary: DictionaryTF,
                 trainingSet: RDD[SparseVector[Int]],
                 testSet: RDD[SparseVector[Int]],
                 sc: SparkContext,
                 gc: Client): Unit = {

    // Transform data set to spark MLLib vectors
    val sparkVectors = trainingSet.zipWithIndex.map(_.swap).map { case (id, x) =>
      (id, Vectors.sparse(x.length, x.activeKeysIterator.toArray, x.activeValuesIterator.map(y => y.toDouble).toArray))
    }.repartition(config.partitions)
    sparkVectors.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val testSparkVectors = testSet.zipWithIndex.map(_.swap).map { case (id, x) =>
      (id, Vectors.sparse(x.length, x.activeKeysIterator.toArray, x.activeValuesIterator.map(y => y.toDouble).toArray))
    }.repartition(config.partitions)
    testSparkVectors.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Construct MLLib LDA model
    val sparkLDAAlgorithm = config.algorithm match {
      case "sparkem" => "em"
      case "sparkonline" => "online"
    }
    val ldaConfig = new LDA().setK(config.topics).setAlpha(config.α + 1.0).setBeta(config.β + 1.0)
      .setSeed(config.seed).setMaxIterations(config.iterations).setOptimizer(sparkLDAAlgorithm)

    // Fit to model
    val ldaModel = sparkLDAAlgorithm match {
      case "em" =>

        // Compute model
        val startTime = System.currentTimeMillis()
        val model = ldaConfig.run(sparkVectors).asInstanceOf[DistributedLDAModel]
        val totalTime = System.currentTimeMillis() - startTime
        println(s"Total training time: ${totalTime}ms")
        val localModel = model.toLocal

        // Compute perplexity
        val trainTokens = sparkVectors.map {
          case (id, v) => v.toSparse.values.sum
        }.sum()
        val trainLogLikelihood = model.logLikelihood
        println(s"Train tokens:         ${trainTokens}")
        println(s"Train log likelihood: ${trainLogLikelihood}")
        println(s"Train perplexity:     ${Math.exp(-trainLogLikelihood / trainTokens.toDouble)}")

        // Run tests if necessary
        if (config.testSize > 0.0) {

          // Construct a new LDA model on the parameter server form the spark LocalLDAModel
          val testTokens = sparkVectors.map {
            case (id, v) => v.toSparse.values.sum
          }.sum()
          val glintLDAConfig = new LDAConfig()
          glintLDAConfig.setTopics(config.topics)
          glintLDAConfig.setVocabularyTerms(config.vocabularySize)
          glintLDAConfig.setα(config.α)
          glintLDAConfig.setβ(config.β)
          glintLDAConfig.setBlockSize(config.blockSize)
          glintLDAConfig.setPartitions(config.partitions)
          val glintModel = LDAModel.fromSpark(localModel, gc, glintLDAConfig)

          // Test perplexity on training and test set
          Solver.test(sc, gc, glintModel, testSet, 20)
        }

        // Return model
        localModel

      case "online" =>

        val startTime = System.currentTimeMillis()

        // Optimal mini batch fraction
        val trainSize = sparkVectors.count().toDouble
        val mbf = (2.0 / config.iterations.toDouble) + (1.0 / trainSize)

        // Run online LDA
        val onlineLdaConfig = ldaConfig.setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(math.min(1.0, mbf)))
        val ldaModel = onlineLdaConfig.run(sparkVectors).asInstanceOf[LocalLDAModel]
        val totalTime = System.currentTimeMillis() - startTime
        println(s"Total training time: ${totalTime}ms")

        // Compute perplexity
        /*val trainTokens = sparkVectors.map {
          case (id, v) => v.toSparse.values.sum
        }.sum()
        val trainLoglikelihood = ldaModel.logLikelihood(sparkVectors)
        println(s"Train tokens:         ${trainTokens}")
        println(s"Train log likelihood: ${trainLoglikelihood}")
        println(s"Train perplexity:     ${Math.exp(-trainLoglikelihood / trainTokens.toDouble)}")*/

        // Test if necessary
        if (config.testSize > 0.0) {

          /*val testTokens = testSparkVectors.map {
            case (id, v) => v.toSparse.values.sum
          }.sum()
          val testLoglikelihood = ldaModel.logLikelihood(testSparkVectors)
          println(s"Test tokens:          ${testTokens}")
          println(s"Test log likelihood:  ${testLoglikelihood}")
          println(s"Test perplexity:      ${Math.exp(-testLoglikelihood / testTokens.toDouble)}")*/
          // Construct a new LDA model on the parameter server form the spark LocalLDAModel
          val testTokens = sparkVectors.map {
            case (id, v) => v.toSparse.values.sum
          }.sum()
          val glintLDAConfig = new LDAConfig()
          glintLDAConfig.setTopics(config.topics)
          glintLDAConfig.setVocabularyTerms(config.vocabularySize)
          glintLDAConfig.setα(config.α)
          glintLDAConfig.setβ(config.β)
          glintLDAConfig.setBlockSize(config.blockSize)
          glintLDAConfig.setPartitions(config.partitions)
          val glintModel = LDAModel.fromSpark(ldaModel, gc, glintLDAConfig)

          // Test perplexity on training and test set
          Solver.test(sc, gc, glintModel, testSet, 20)
        }

        ldaModel
    }

    // Print top 50 topics
    var topicNumber = 1
    val inverseMap = dictionary.mapping.map(_.swap)
    ldaModel.describeTopics(50).foreach {
      case (ks, ps) =>
        println(s"Topic ${topicNumber}")
        topicNumber += 1
        ks.zip(ps).foreach {
          case (k, p) =>  println(s"   ${inverseMap(k)}:         ${p}")
        }
    }

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
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(
      classOf[SparseVector[Int]],
      classOf[SparseVector[Long]],
      classOf[SparseVector[Double]],
      classOf[SparseVector[Float]]
    ))
    glintlda.util.registerKryo(sparkConf)
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

