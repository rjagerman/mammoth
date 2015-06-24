package ch.ethz.inf.da.mammoth.topicmodeling

import java.io.File

import breeze.linalg._
import breeze.stats.distributions.{RandBasis, Uniform}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.mllib.feature.DictionaryTF

/**
 * A topic model
 *
 * @param β The topic-word probabilities
 */
class TopicModel(val β: DenseMatrix[Double]) {

  val topics = β.cols
  val features = β.rows

  /**
   * Returns the topics found by the topic model as a 2-dimensional list of word probability
   *
   * @param dictionary The dictionary to use for the inverse transform
   * @param n The maximum number of terms to return for a topic (returns the top n)
   * @return The top n terms from the topic model
   */
  def topics(dictionary: DictionaryTF, n: Int = 20): List[List[(Any, Double)]] = {
    val inverseDictionary = dictionary.mapping.map(x => x.swap)
    (0 until β.cols).map(
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
    this.topics(dictionary, n).zipWithIndex.foreach {
      case (x, k) => println(s"Topic $k"); x.foreach {
        case (w, value) => println(s"  $w " + " " * math.max(1, 30 - s"$w".length()) + s" $value")
      }
    }
  }

}

/**
 * Object with utility functions for topic models
 */
object TopicModel {

  /**
   * Writes the topic model to disk
   *
   * @param topicModel The topic model
   * @param filename The file name
   */
  def write(topicModel: TopicModel, filename: String) = {
    breeze.linalg.csvwrite(new File(filename), topicModel.β, separator = ' ')
  }

  /**
   * Reads the topic model from disk
   *
   * @param filename The file name
   * @return The topic model
   */
  def read(filename: String): TopicModel = new TopicModel(breeze.linalg.csvread(new File(filename), separator = ' '))

  /**
   * Generates a random topic model from given seed
   *
   * @param features The number of features
   * @param topics The number of topics
   * @param seed The random seed to use
   * @return The topic model
   */
  def random(features: Int, topics: Int, seed: Int): TopicModel = {
    var β = DenseMatrix.rand[Double](features, topics, new Uniform(0, 1)(new RandBasis(new MersenneTwister(seed))))
    β = β(*, ::) :/ sum(β(::, *)).toDenseVector
    new TopicModel(β)
  }


}
