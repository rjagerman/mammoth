package ch.ethz.inf.da.mammoth.topicmodeling

import breeze.linalg._
import org.apache.spark.mllib.feature.DictionaryTF
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Distributed topic model
 *
 * @param topics The number of topics
 * @param features The number of features (i.e. dimensionality) of your input data
 * @param globalIterations The number of global iterations
 * @param localIterations The number of local iterations
 */
class DistributedTopicModel(private var features: Int,
                            private var topics: Int,
                            private var globalIterations: Int,
                            private var localIterations: Int) extends Serializable {

  /**
   * Fits given documents to a topic model model
   *
   * @param documents The documents to fit
   */
  def fit(documents: RDD[SparseVector[Double]], dictionary: DictionaryTF, initialModel: TopicModel): TopicModel = {

    // Get the topic-word probabilities from the initial model
    var β = initialModel.β

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

      // Print topic model and save it to disk
      println(s"Topics after iteration $t")
      val topicModel = new TopicModel(β)
      topicModel.printTopics(dictionary, 10)
      TopicModel.write(topicModel, s"models/iter${t}.model")

    }

    new TopicModel(β)
  }

}
