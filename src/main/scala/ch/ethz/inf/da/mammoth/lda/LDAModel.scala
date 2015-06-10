package ch.ethz.inf.da.mammoth.lda

import breeze.linalg._
import breeze.stats.distributions.{RandBasis, Uniform}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.mllib.feature.DictionaryTF

/**
 * An LDA model
 *
 * @param topics The number of topics
 * @param features The number of features (terms in the vocabulary)
 * @param documents The number of documents
 * @param β The topic-word probabilities
 */
class LDAModel(val topics: Int,
               val features: Int,
               var documents: Long,
               var β: DenseMatrix[Double]) extends Serializable {

  /**
   * Adds two LDA models together by summing their topic word distributions
   *
   * @param that The other LDA model
   * @return This LDA model with updated β
   */
  def +(that: LDAModel): LDAModel = {
    new LDAModel(topics, features, this.documents + that.documents, this.β :+ that.β)
  }

  /**
   * Normalizes the topic model and makes sure that each topic's word distribution sums up to 1.0
   */
  def normalize(): Unit = {
    this.β = β(*, ::) :/ sum(β(::, *)).toDenseVector // Normalizes by columns (columns sum to 1)
  }

  /**
   * Returns the topics found by the LDA model as a 2-dimensional list of word probability
   *
   * @param dictionary The dictionary to use for the inverse transform
   * @param n The maximum number of terms to return for a topic (returns the top n)
   * @return The top n terms from the dictionary
   */
  def topics(dictionary: DictionaryTF, n: Int = 20): List[List[(Any, Double)]] = {
    val inverseDictionary = dictionary.mapping.map(x => x.swap)
    (0 until topics).map(
      k => argtopk(β(::, k), n).map(j => (inverseDictionary(j), β(j, k))).toList
    ).toList
  }

  /**
   * Prints the top n terms of each topic to the standard output
   *
   * @param dictionary The dictionary to use for the inverse transform
   * @param n The maximum number of terms to print
   */
  def printTopics(dictionary: DictionaryTF, n: Int = 20): Unit = {
    this.topics(dictionary, 10).zipWithIndex.foreach {
      case (x, k) => println(s"Topic $k"); x.foreach {
        case (w, value) => println(s"  $w " + " "*math.max(1, 30 - s"$w".length()) + s" $value")
      }
    }
  }

}

/**
 * Object with utility functions for LDA Model
 */
object LDAModel {

  /**
   * Creates a randomly initialized LDA model
   *
   * @param topics The number of topics
   * @param features The number of features
   * @param documents The number of documents
   * @param seed The random seed used to initialize the model
   * @return A randomly initialized LDA model
   */
  def random(topics:Int, features:Int, documents:Long = 0, seed:Int = 42): LDAModel = {
    val β = DenseMatrix.rand[Double](features, topics, new Uniform(0, 1)(new RandBasis(new MersenneTwister(seed))))
    new LDAModel(topics, features, documents, β)
  }

}
