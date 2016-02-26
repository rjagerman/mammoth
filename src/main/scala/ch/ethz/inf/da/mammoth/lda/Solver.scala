package ch.ethz.inf.da.mammoth.lda

import java.util.concurrent.Semaphore

import akka.util.Timeout
import breeze.linalg.SparseVector
import ch.ethz.inf.da.mammoth.util.{SSPClock, FastRNG, RDDImprovements}
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
  * @param sspClock The stale-synchronous protocol clock
  * @param id The identifier
  */
abstract class Solver(model: LDAModel, sspClock: SSPClock, id: Int) {

  // Construct execution context, timeout and logger
  implicit protected val ec = ExecutionContext.Implicits.global
  implicit protected val timeout = new Timeout(180 seconds)
  protected val logger: Logger = Logger(LoggerFactory getLogger s"${getClass.getSimpleName}-$id")

  /**
    * Initializes the count table on the parameter servers specified in model with given partition of the data
    *
    * @param samples The samples to initialize with
    */
  private def initialize(samples: Array[GibbsSample]): Unit = {

    // Initialize buffered matrix for word topic counts
    val buffer = new BufferedBigMatrix[Long](model.wordTopicCounts, 20000)
    val topics = new Array[Long](model.config.topics)
    val pushLock = new Semaphore(256)

    // Iterate over all samples and the corresponding features, counting them and pushing to the parameter servers
    var i = 0
    while (i < samples.length) {
      val sample = samples(i)

      var j = 0
      while (j < sample.features.length) {

        if (buffer.isFull) {
          pushLock.acquire()
          buffer.flush().onComplete(_ => pushLock.release())
        }
        buffer.pushToBuffer(sample.features(j), sample.topics(j), 1)
        topics(sample.topics(j)) += 1

        j += 1
      }

      i += 1
    }

    // Perform final flush and await results to guarantee everything has been processed on the parameter servers
    pushLock.acquire(2)
    buffer.flush().onComplete(_ => pushLock.release())
    model.topicCounts.push((0L until model.config.topics).toArray, topics).onComplete(_ => pushLock.release())
    pushLock.acquire(256)
    pushLock.release(256)

  }

  /**
    * Runs the LDA inference algorithm on given partition of the data
    *
    * @param samples The samples to run the algorithm on
    * @param evaluation The evaluation reference to add measurements to
    */
  private def fit(samples: Array[GibbsSample], evaluation: Evaluation): Unit = {

    // Sleep to spread out the start of the routines (to prevent flooding the parameter servers at the start)
    Thread.sleep(id * 100)

    // Perform iterations
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
          solver: (LDAModel, SSPClock, Int) => Solver): LDAModel = {

    // Transform data to gibbs samples
    val gibbsSamples = transform(samples, config)

    // Execution context and timeouts for asynchronous operations
    implicit val ec = ExecutionContext.Implicits.global
    implicit val timeout = new Timeout(120 seconds)

    // SSP Clock
    //val sspClock = SSPClock(gc, config.partitions)

    // Construct LDA model and initialize it on the parameter server
    val model = LDAModel(gc, config, new CyclicalFeatureMap(config.vocabularyTerms, config.partitions))
    gibbsSamples.foreachPartitionWithIndex { case (id, it) =>
      val s = solver(model, null, id)
      s.initialize(it.toArray)
    }

    // Construct evaluation
    val evaluation = Evaluation(gc, model)
    val evaluationFuture = evaluation.asyncPrintLoop(60 seconds) // Check state every 60 seconds and print perplexity

    // Perform training
    gibbsSamples.foreachPartitionWithIndex { case (id, it) =>
      val s = solver(model, null, id)
      s.fit(it.toArray, evaluation)
    }

    // Wait for evaluation print loop to finish
    Await.result(evaluationFuture, (15 * 60) seconds)

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
      val cyclicalFeatureMap = new CyclicalFeatureMap(config.vocabularyTerms, config.partitions)
      it.map(s => GibbsSample(s, random, config.topics, cyclicalFeatureMap))
    }.repartition(config.partitions).persist(StorageLevel.MEMORY_AND_DISK)

    // Trigger empty action to materialize the mapping and persist it
    gibbsSamples.foreachPartition(_ => ())
    gibbsSamples
  }

}
