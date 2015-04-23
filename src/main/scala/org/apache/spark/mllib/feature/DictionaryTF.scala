package org.apache.spark.mllib.feature

import java.lang.{Iterable => JavaIterable}

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * :: Experimental ::
 * Maps a sequence of terms to their term frequencies using a limited-size dictionary that takes the most frequent terms
 *
 * @param mapping A map from terms (Any) to integer indices
 * @param numFeatures The number of features
 */
@Experimental
class DictionaryTF(val mapping: Map[Any, Int], numFeatures: Int) extends Serializable {

  /**
   * Returns the index of the input term.
   */
  def indexOf(term: Any): Int = this.mapping(term)

  /**
   * Checks if an index for given term exists in the dictionary.
   */
  def hasIndex(term: Any): Boolean = this.mapping.keySet.contains(term)

  /**
   * Transforms the input document into a sparse term frequency vector.
   */
  def transform(document: Iterable[_]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    document.filter {
      x => hasIndex(x)
    } foreach { term =>
      val i = indexOf(term)
      termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
    }
    Vectors.sparse(numFeatures, termFrequencies.toSeq)
  }

  /**
   * Transforms the input document into a sparse term frequency vector (Java version).
   */
  def transform(document: JavaIterable[_]): Vector = {
    transform(document.asScala)
  }

  /**
   * Transforms the input document to term frequency vectors.
   */
  def transform[D <: Iterable[_]](dataset: RDD[D]): RDD[Vector] = {
    dataset.map(this.transform)
  }

  /**
   * Transforms the input document to term frequency vectors (Java version).
   */
  def transform[D <: JavaIterable[_]](dataset: JavaRDD[D]): JavaRDD[Vector] = {
    dataset.rdd.map(this.transform).toJavaRDD()
  }
}