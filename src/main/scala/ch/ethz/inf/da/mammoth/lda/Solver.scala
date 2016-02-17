package ch.ethz.inf.da.mammoth.lda

import java.util.concurrent.Semaphore

import akka.util.Timeout
import breeze.linalg.SparseVector
import ch.ethz.inf.da.mammoth.util.FastRNG
import ch.ethz.inf.da.mammoth.util.RDDImprovements
import com.typesafe.scalalogging.slf4j.Logger
import glint.Client
import glint.models.client.buffered.BufferedBigMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._

/**
  * A solver that can compute an LDA model based on data
  *
  * @param model The LDA model
  * @param id The identifier
  */
abstract class Solver(model: LDAModel, id: Int) {

  // Construct execution context, timeout and logger
  implicit protected val ec = ExecutionContext.Implicits.global
  implicit protected val timeout = new Timeout(300 seconds)
  protected val logger: Logger = Logger(LoggerFactory getLogger s"${getClass.getSimpleName}-$id")

  /**
    * Initializes the count table on the parameter servers specified in model with given partition of the data
    *
    * @param samples The samples to initialize with
    */
  private def initialize(samples: Array[GibbsSample]): Unit = {

    // Initialize buffered matrix for word topic counts
    val buffer = new BufferedBigMatrix[Long](model.wordTopicCounts, 10000)
    val topics = new Array[Long](model.config.topics)
    val pushLock = new Semaphore(256)

    // Iterate over all samples and the corresponding features, counting them and pushing to the parameter servers
    var i = 0
    while (i < samples.length) {
      val sample = samples(i)

      var j = 0
      while (j < sample.features.length) {

        buffer.pushToBuffer(sample.features(j), sample.topics(j), 1)
        topics(sample.topics(j)) += 1
        if (buffer.isFull) {
          pushLock.acquire()
          buffer.flush().onComplete(_ => pushLock.release())
        }

        j += 1
      }

      i += 1
    }

    // Perform final flush and await results to guarantee everything has been processed on the parameter servers
    Await.result(buffer.flush(), timeout.duration.toMillis milliseconds)
    Await.result(model.topicCounts.push((0L until model.config.topics).toArray, topics),
      timeout.duration.toMillis milliseconds)
  }

  /**
    * Runs the LDA inference algorithm on given partition of the data
    *
    * @param samples The samples to run the algorithm on
    * @param evaluation The evaluation reference to add measurements to
    */
  private def fit(samples: Array[GibbsSample], evaluation: Evaluation): Unit = {
    var t = 0
    while (t < model.config.iterations) {
      t += 1
      logger.info(s"Starting iteration $t/${model.config.iterations}")
      fit(samples, evaluation, t)
    }
  }

  /**
    * Runs the LDA inference algorithm on given partition of the data
    *
    * @param samples The samples to run the algorithm on
    * @param evaluation The evaluation reference to add measurements to
    * @param iteration The iteration number
    */
  protected def fit(samples: Array[GibbsSample], evaluation: Evaluation, iteration: Int): Unit

}

/**
  * The solver
  */
object Solver {

  /**
    * Runs the solver
    *
    * @param gc The glint client
    * @param samples The samples as word-frequency vectors
    * @param config The LDA configuration
    * @return A trained LDA model
    */
  def fit(gc: Client,
          samples: RDD[SparseVector[Int]],
          config: LDAConfig,
          solver: (LDAModel, Int) => Solver): LDAModel = {

    // Transform data to gibbs samples
    val gibbsSamples = transform(samples, config)

    // Construct LDA model and initialize it on the parameter server
    val model = LDAModel(gc, config)
    gibbsSamples.foreachPartitionWithIndex { case (id, it) =>
      val s = solver(model, id)
      s.initialize(it.toArray)
    }

    // Construct evaluation
    implicit val ec = ExecutionContext.Implicits.global
    implicit val timeout = new Timeout(60 seconds)
    val evaluation = Evaluation(gc, config)
    evaluation.asyncPrintLoop(60 seconds) // Check state every 60 seconds and print perplexity when possible

    // Perform training
    gibbsSamples.foreachPartitionWithIndex { case (id, it) =>
      val s = solver(model, id)
      s.fit(it.toArray, evaluation)
    }

    // Return trained model
    model
  }

  /**
    * Transforms given samples into (randomly initialized) Gibbs samples
    *
    * @param samples The samples as word-frequency vectors
    * @param config The LDA configuration
    * @return An RDD containing Gibbs samples
    */
  private def transform(samples: RDD[SparseVector[Int]], config: LDAConfig): RDD[GibbsSample] = {

    // Map partitions to Gibbs samples that have a random initialization
    val gibbsSamples = samples.mapPartitionsWithIndex { case (id, it) =>
      val random = new FastRNG(config.seed + id)
      it.map(s => GibbsSample(s, random, config.topics))
    }.repartition(config.partitions).persist(StorageLevel.MEMORY_AND_DISK)

    // Trigger empty action to materialize the mapping and persist it
    gibbsSamples.foreachPartition(_ => ())
    gibbsSamples
  }

}
