package ch.ethz.inf.da.mammoth.topicmodeling.em

import breeze.linalg._
import ch.ethz.inf.da.mammoth.util.{intPairToLong, longToIntPair}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.DictionaryTF
import org.apache.spark.rdd.RDD

/**
 * Distributed topic model
 *
 * @param topics The number of topics
 * @param features The number of features (i.e. dimensionality) of your input data
 * @param globalIterations The number of global iterations
 * @param localIterations The number of local iterations
 * @param sc The spark context (used to broadcast the topic model for each iteration)
 */
class DistributedEMSolver(private var features: Int,
                            private var topics: Int,
                            private var globalIterations: Int,
                            private var localIterations: Int,
                            @transient val sc: SparkContext) extends Serializable {

  /**
   * Fits given documents to a topic model
   *
   * @param documents The documents to fit
   * @param dictionary The dictionary (used to print out the topics for each iteration)
   * @param initialModel The model to start with (typically random or loaded from file)
   */
  def fit(documents: RDD[SparseVector[Double]], dictionary: DictionaryTF, initialModel: TopicModel): TopicModel = {

    // Get the topic-word probabilities from the initial model
    var β = initialModel.β

    // Perform LDA with multiple iterations. Each iterations computes the local topic models for all partitions of the
    // data set and combines them into a global topic model.
    for (t <- 1 to globalIterations) {
      println(s"Global iteration $t")

      // Broadcast current topic model
      val β_broadcasted = sc.broadcast(β)

      // Map each partition to a local LDA Model
      val models = documents.mapPartitions(partitionData => {
        println("Obtaining β_broadcasted")
        val β_local = β_broadcasted.value
        println("Obtaining data")
        val data = partitionData.toArray
        println("Constructing LDA solver")
        val solver = new LDASolver(localIterations, data, β_local)
        println("Executing solve()")
        solver.solve()
        println("Emitting matrix")
        β_local.activeIterator.map{ case ((x, y), v) => (intPairToLong((x,y)), v) } // Convert (int, int) => long
      })

      // Reduce the LDA Models into a single LDA Model used for the next iteration
      // Here the topic-word probabilities for each local model are summed up
      val β_next = DenseMatrix.zeros[Double](features, topics)
      models.reduceByKey(_ + _).collect.map {
        case (c, v) => (longToIntPair(c), v) // convert long => (int, int)
      }.foreach {
        case ((x, y), v) => β_next(x, y) = v
      }

      // Normalize the model so the topic-word probabilities add up to 1 for each topic
      β = β_next(*, ::) :/ sum(β_next(::, *)).toDenseVector // Normalizes by columns (columns sum to 1)

      // Print topic model
      println(s"Topics after iteration $t")
      val topicModel = new TopicModel(β)
      topicModel.printTopics(dictionary, 10)

    }

    new TopicModel(β)
  }

}
