package ch.ethz.inf.da.mammoth

import ch.ethz.inf.da.mammoth.warc.Document
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Dictionary generating functions
 */
package object dictionary {

  /**
   * Generates a dictionary by taking the n most frequent words
   * 
   * @param documents The documents as an RDD
   * @param n The number of words to take
   * @return A mapping from strings to indices
   */
  def topFrequencyDictionary(documents:RDD[Document[Array[String]]], n:Int): Map[String, Int] = {
    documents.flatMap(doc => doc.contents)
      .map(word ⇒ (word, 1))        // Map each individual word to the count 1
      .reduceByKey(_ + _)           // Reduce by summing the word counts
      .map(item ⇒ item.swap)        // Swap (word,count) to (count,word)
      .sortByKey(false, 1)          // Sort by key (the counts)
      .map(item ⇒ item.swap)        // Swap (count,word) back to (word,count)
      .take(n)                      // Only take the top maxVocabularySize words
      .map(_._1).zipWithIndex.toMap // Convert to a map with indices
  }

}
