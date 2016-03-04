package org.apache.spark.mllib.feature

import org.apache.spark.SparkContext._
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Dictionary
 * This is a limited-size dictionary. It filters out the least common terms, such that the
 * words in the dictionary are guaranteed to be the top n most frequent terms.
 *
 * @param numFeatures Retains this number of words in the dictionary by computing the most
 *                    frequent terms
 */
@Experimental
class Dictionary(numFeatures: Int) {

  def this() = this(1 << 20)

  /**
   * Fits the dictionary to given dataset
   * @return A dictionary transformer fitted on given dataset
   */
  def fit[D <: Iterable[_]](dataset: RDD[D]) = {

    val dictionaryData = dataset.flatMap(doc => doc)
      .map(word ⇒ (word, 1L))         // Map each individual word to the count 1
      .reduceByKey(_ + _)            // Reduce by summing the word counts
      .map(item ⇒ item.swap)         // Swap (word,count) to (count,word)
      .sortByKey(false, 1)           // Sort by key (the counts)
      .map(item ⇒ item.swap)         // Swap (count,word) back to (word,count)
      .take(numFeatures)             // Only take the top n words

    dictionaryData.foreach { case (word, count) =>
      println(s"${word}, ${count}")
    }

    val map = dictionaryData.map(_._1).zipWithIndex.toMap  // Convert to a map with indices

    new DictionaryTF(map, numFeatures)
  }

}
