package ch.ethz.inf.da.mammoth.lda

import akka.util.Timeout
import ch.ethz.inf.da.mammoth.util._
import glint.Client
import glint.models.client.retry.{RetryBigVector, RetryBigMatrix}
import glint.models.client.{BigVector, BigMatrix}
import org.apache.spark.mllib.feature.DictionaryTF

import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._

/**
  * A distributed LDA Model
  *
  * @param wordTopicCounts A big matrix representing the topic word counts
  * @param topicCounts A big vector representing the global topic counts
  * @param config The LDA configuration
  */
class LDAModel(var wordTopicCounts: BigMatrix[Long],
               var topicCounts: BigVector[Long],
               val config: LDAConfig,
               cyclicalFeatureMap: CyclicalFeatureMap) extends Serializable {

  /**
    * Prints out the top words of the topic model for each topic
    *
    * @param words The number of words to print
    * @param dictionary The dictionary to use
    */
  def describe(words: Int, dictionary: DictionaryTF)(implicit ec: ExecutionContext, timeout: Timeout): Unit = {
    val topWords = getTopWords(words, dictionary)
    for (t <- 0 until config.topics) {
      println(s"========= TOPIC ${t + 1} =========")
      topWords(t).foreach {
        case (word, probability) => println(s"  $word " + " " * math.max(1, 30 - word.length()) + s" $probability")
      }
      println()
    }
  }

  /**
    * Gets the top n words for each topic in the topic model
    *
    * @param words The number of words to return
    * @param dictionary A dictionary to translate strings to indices and vice versa
    * @return Array of a list of words for each topic
    */
  def getTopWords(words: Int,
                  dictionary: DictionaryTF)(implicit ec: ExecutionContext,
                                            timeout: Timeout): Array[List[(String, Double)]] = {

    val topWords = Array.fill(config.topics)(new BoundedPriorityQueue[(Double, Long)](words))
    val globalCounts = Await.result(topicCounts.pull((0L until config.topics).toArray), timeout.duration)
    ranges(0, config.vocabularyTerms, 1000).foreach { case range =>
      val rangeArray = range.toArray
      val mappedRangeArray = range.map(x => cyclicalFeatureMap.map(x.toInt).toLong).toArray
      val rows = Await.result(wordTopicCounts.pull(mappedRangeArray), timeout.duration)
      rangeArray.zip(rows).foreach {
        case (feature, row) =>
          for (t <- 0 until config.topics) {
            val p = (config.β + row(t).toDouble) / (config.vocabularyTerms * config.β + globalCounts(t).toDouble)
            topWords(t).enqueue((p, feature))
          }
      }
    }
    val invertedDictionary = dictionary.mapping.map { case (x, y) => (y, x.toString) }
    topWords.map {
      case bpq => bpq.iterator.toList.sorted.reverse.map { case (prob, f) => (invertedDictionary(f.toInt), prob) }
    }
  }

}

object LDAModel {

  /**
    * Constructs an empty LDA model based on given configuration
    *
    * @param config The LDA configuration
    */
  def apply(gc: Client, config: LDAConfig, cyclicalFeatureMap: CyclicalFeatureMap): LDAModel = {
    val topicWordCounts = new RetryBigMatrix[Long](gc.matrix[Long](config.vocabularyTerms, config.topics, 2))
    val globalCounts = new RetryBigVector[Long](gc.vector[Long](config.topics, 1))
    new LDAModel(topicWordCounts, globalCounts, config, cyclicalFeatureMap)
  }

}

