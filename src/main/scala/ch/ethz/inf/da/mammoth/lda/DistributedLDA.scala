package ch.ethz.inf.da.mammoth.lda

import breeze.linalg._
import breeze.stats.distributions.{RandBasis, Uniform}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.{SparkContext, AccumulableParam, Accumulable}
import org.apache.spark.mllib.feature.DictionaryTF
import org.apache.spark.mllib.linalg.{DenseMatrix => DM}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

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
  def fit(documents: RDD[SparseVector[Double]], dictionary: DictionaryTF, seed: Int = 42): LDAModel = {

    // Initialize topic model with random topics
    var β = DenseMatrix.rand[Double](features, topics, new Uniform(0, 1)(new RandBasis(new MersenneTwister(seed))))

    // Perform LDA with multiple iterations. Each iterations computes the local topic models for all partitions of the
    // data set and combines them into a global topic model.
    for (t <- 1 to globalIterations) {
      println(s"Global iteration $t")

      // Map each partition x to a local LDA Model
      val models = documents.mapPartitions(partitionData => {
        val solver = new LDASolver(localIterations, partitionData.toArray, β)
        solver.solve()
        β.activeIterator.map{ case ((x,y), v) => ((x.toLong << 32) + y.toLong, v) } // Convert (int,int) => long
      })

      // Reduce the LDA Models into a single LDA Model used for the next iteration
      // Here the topic-word probabilities for each local model are summed up
      val β_next = DenseMatrix.zeros[Double](features, topics)
      models.reduceByKey(_ + _).collect.foreach {
        case (coordinate, v) => β_next((coordinate >> 32).toInt, coordinate.toInt) = v // Convert long => (int,int)
      }

      // Normalize the model so the topic-word probabilities add up to 1 for each topic
      β = β_next(*, ::) :/ sum(β_next(::, *)).toDenseVector // Normalizes by columns (columns sum to 1)

      // Print topics
      println(s"Topics after iteration $t")
      LDAModel.printTopics(β, dictionary, 10)
      LDAModel.save(β, s"models/iter${t}.model")

    }

    new LDAModel(topics, features, 0, β)
  }

}
