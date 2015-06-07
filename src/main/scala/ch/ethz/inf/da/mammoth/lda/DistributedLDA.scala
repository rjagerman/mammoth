package ch.ethz.inf.da.mammoth.lda

import breeze.linalg.{Matrix, SparseVector, DenseVector}
import org.apache.spark.mllib.feature.DictionaryTF
import org.apache.spark.rdd.RDD

/**
 * Distributed LDA implementation
 *
 * @param topics The number of topics
 * @param features The number of features (i.e. dimensionality) of your input data
 * @param globalIterations The number of global iterations
 * @param localIterations The number of local iterations
 */
class DistributedLDA(private var features: Int,
                     private var topics: Int,
                     private var globalIterations: Int,
                     private var localIterations: Int) extends Serializable {

  /**
   * Default constructor
   * @param features The number of features (i.e. dimensionality) of your input data
   */
  def this(features: Int) = this(features = features, topics = 10, globalIterations = 10, localIterations = 5)

  /**
   * Number of topics to infer.  I.e., the number of soft cluster centers.
   */
  def getTopics: Int = topics

  /**
   * Number of topics to infer.  I.e., the number of soft cluster centers.
   * (default = 10)
   */
  def setTopics(topics: Int): this.type = {
    require(topics > 0, s"LDA topics (number of clusters) must be > 0, but was set to $topics")
    this.topics = topics
    this
  }

  /**
   * Number of features
   */
  def getFeatures: Int = features

  /**
   * Number of features
   * (default = 1000)
   */
  def setFeatures(features: Int): this.type = {
    require(features > 0, s"LDA features (number of unique terms) must be > 0, but was set to $features")
    this.features = features
    this
  }

  /**
   * Number of global iterations
   */
  def getGlobalIterations: Int = globalIterations

  /**
   * Number of global iterations
   * (default = 10)
   */
  def setGlobalIterations(globalIterations: Int): this.type = {
    require(globalIterations > 0, s"LDA global iterations must be > 0, but was set to $globalIterations")
    this.globalIterations = globalIterations
    this
  }

  /**
   * Number of local iterations
   */
  def getLocalIterations: Int = localIterations

  /**
   * Number of local iterations
   * (default = 5)
   */
  def setLocalIterations(localIterations: Int): this.type = {
    require(localIterations > 0, s"LDA global iterations must be > 0, but was set to $localIterations")
    this.localIterations = localIterations
    this
  }

  /**
   * Fits given documents to an LDA model
   *
   * @param documents The documents to fit
   */
  def fit(documents: RDD[SparseVector[Double]], dictionary: DictionaryTF): LDAModel = {

    // Initialize model with random topics
    var model = LDAModel.createRandomModel(topics, features, 0)

    // Perform LDA with multiple iterations. Each iterations computes the local topic models for all partitions of the
    // data set and combines them into a global topic model.
    for (t <- 1 to globalIterations){
      println(s"Global iteration ${t}")

      // Map each partition to a local LDA Model
      val li = localIterations
      val models = documents.mapPartitions(x => {
        val solver = new LDASolver(li, x.toArray, model)
        solver.solve()
        Array.fill(1) { model }.toIterator
      })

      // Reduce the LDA Models into a single LDA Model used for the next iteration
      model = models.reduce( _ + _ )

      // Normalize the model so the topic-word probabilities add up to 1 for each topic
      model.normalize()

      // Print topics
      println(s"Topics after iteration $t")
      model.printTopics(dictionary, 10)

    }

    model
  }

}