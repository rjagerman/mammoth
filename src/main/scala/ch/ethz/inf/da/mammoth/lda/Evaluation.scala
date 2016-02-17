package ch.ethz.inf.da.mammoth.lda

import akka.util.Timeout
import breeze.numerics._
import breeze.linalg.Vector
import com.typesafe.scalalogging.slf4j.Logger
import glint.Client
import glint.models.client.BigVector
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, blocking}

/**
  * Distributed evaluation for LDA
  *
  * @param config The LDA configuration
  * @param docLogLikelihood The (per iteration) document log likelihood
  * @param wordLogLikelihood The (per iteration) word log likelihood
  * @param tokenCounts The (per iteration) token counts
  * @param partitionCounts The (per iteration) partition counts
  */
class Evaluation(val config: LDAConfig,
                 var docLogLikelihood: BigVector[Double],
                 var wordLogLikelihood: BigVector[Double],
                 var tokenCounts: BigVector[Long],
                 var partitionCounts: BigVector[Int]) extends Serializable {

  /**
    * Adds the local document likelihood
    *
    * @param iteration The iteration number
    * @param samples The partition of samples
    */
  def addLocalDocLikelihood(iteration: Int,
                            samples: Array[GibbsSample])(implicit ec: ExecutionContext, timeout: Timeout): Unit = {

    // Initialize
    var localDocLikelihood: Double = 0.0
    var localTokenCounts: Long = 0

    // Add all document-dependent likelihood computations
    localDocLikelihood += samples.length * lgamma(config.α * config.topics)
    var i = 0
    while (i < samples.length) {
      val sample = samples(i)
      localTokenCounts += sample.features.length
      localDocLikelihood -= lgamma(config.α * config.topics + sample.features.length)
      val sparseCounts = sample.sparseCounts(config.topics)
      var offset = 0
      while( offset < sparseCounts.activeSize) {
        val index: Int = sparseCounts.indexAt(offset)
        val value: Int = sparseCounts.valueAt(offset)
        localDocLikelihood += lgamma(config.α + value)
        offset += 1
      }
      localDocLikelihood += (config.topics - sparseCounts.activeSize) * lgamma(config.α)
      i += 1
    }
    localDocLikelihood -= samples.length * config.topics * lgamma(config.α)

    // Push resulting values to parameter server for aggregation
    Await.result(docLogLikelihood.push(Array(iteration - 1), Array(localDocLikelihood)), timeout.duration)
    Await.result(tokenCounts.push(Array(iteration - 1), Array(localTokenCounts)), timeout.duration)
  }

  /**
    * Adds the local word likelihood
    *
    * @param iteration The iteration number
    * @param wordTopicCounts The word topic counts
    */
  def addLocalWordLikelihood(iteration: Int,
                             wordTopicCounts: Array[Vector[Long]])(implicit ec: ExecutionContext, timeout: Timeout): Unit = {

    // Initialize
    var localWordLikelihood: Double = 0.0

    // Add all word-dependent likelihood computations
    var i = 0
    while (i < wordTopicCounts.length) {
      val wordTopicCount = wordTopicCounts(i)
      var j = 0
      while (j < wordTopicCount.length) {
        localWordLikelihood += lgamma(config.β + wordTopicCount(j))
        j += 1
      }
      i += 1
    }

    // Push resulting value to parameter server for aggregation
    wordLogLikelihood.push(Array(iteration - 1), Array(localWordLikelihood))
  }

  /**
    * Finishes the evaluation update for this partition
    *
    * @param iteration The iteration number
    * @param global The global array
    */
  def finish(iteration: Int, global: Array[Long])(implicit ec: ExecutionContext, timeout: Timeout): Unit = {

    // Initialize
    var localLikelihood: Double = 0.0

    // Compute normalized global likelihood
    var i = 0
    while (i < global.length) {
      localLikelihood -= lgamma(config.vocabularyTerms * config.β + global(i).toDouble)
      i += 1
    }

    // Push resulting value to parameter server for aggregation
    Await.result(wordLogLikelihood.push(Array(iteration - 1), Array(localLikelihood)), timeout.duration)
    Await.result(partitionCounts.push(Array(iteration - 1), Array(1)), timeout.duration)
  }

  /**
    * Performs an asynchronous loop (this function does not block) attempting to print out evaluation metrics
    * such as perplexity whenever an iteration of the algorithm has finished.
    *
    * @param delay Delay between updates (in seconds)
    */
  def asyncPrintLoop(delay: Duration)(implicit ec: ExecutionContext, timeout: Timeout): Unit = {
    asyncPrintLoop(0, 0, delay)
  }

  /**
    * Performs an asynchronous loop (this function does not block) attempting to print out evaluation metrics
    * such as perplexity whenever an iteration of the algorithm has finished.
    *
    * @param iteration The current iteration number
    * @param previousFinishedPartitions The previous number of finished partitions
    * @param delay Delay between updates (in seconds)
    */
  private def asyncPrintLoop(iteration: Int = 0,
                             previousFinishedPartitions: Int = 0,
                             delay: Duration)(implicit ec: ExecutionContext, timeout: Timeout): Unit = {
    Future { blocking {
      if (iteration < config.iterations) {
        val currentFinishedPartitions = Await.result(partitionCounts.pull(Array(iteration.toLong)), 300 seconds)(0)
        var nextIteration = iteration
        if (currentFinishedPartitions > previousFinishedPartitions) {
          val logger = Logger(LoggerFactory getLogger s"${getClass.getSimpleName}")
          logger.info(s"Iteration ${iteration + 1}, partitions finished ${currentFinishedPartitions} / ${config.partitions}")
        }
        if (currentFinishedPartitions == config.partitions) {
          printState(iteration)
          nextIteration += 1
        }
        Thread.sleep(delay.toMillis)
        asyncPrintLoop(nextIteration, currentFinishedPartitions, delay)
      }
    }}
  }

  /**
    * Prints the state at given iteration
    *
    * @param iteration The iteration
    */
  private def printState(iteration: Int)(implicit ec: ExecutionContext, timeout: Timeout): Unit = {

    // Get iterations document loglikelihood, word loglikelihood and token counts
    val dllh = Await.result(docLogLikelihood.pull(Array(iteration.toLong)), 300 seconds)(0)
    val wllh = Await.result(wordLogLikelihood.pull(Array(iteration.toLong)), 300 seconds)(0) / config.partitions.toDouble
    val tc = Await.result(tokenCounts.pull(Array(iteration.toLong)), 300 seconds)(0)

    // Compute loglikelihood and normalize
    var loglikelihood = dllh + wllh
    loglikelihood += config.topics * lgamma(config.vocabularyTerms * config.β)
    loglikelihood -= config.topics * config.vocabularyTerms * lgamma(config.β)

    // Compute perplexity
    val perplexity = Math.exp(-loglikelihood / tc)

    // Print to log
    val logger = Logger(LoggerFactory getLogger s"${getClass.getSimpleName}")
    logger.info(s"Evaluation after iteration ${iteration + 1}")
    logger.info(s"Doc log-likelihood:  ${dllh}")
    logger.info(s"Word log-likelihood: ${wllh}")
    logger.info(s"Norm log-likelihood: ${loglikelihood}")
    logger.info(s"Token counts:        ${tc}")
    logger.info(s"Perplexity:          ${perplexity}")
  }

}

/**
  * Singleton for constructing Evaluation objects
  */
object Evaluation {

  /**
    * Creates a new Evaluation object on given gc's parameter servers
    *
    * @param gc The glint client
    * @param config The LDA configuration
    * @return The evaluation
    */
  def apply(gc: Client, config: LDAConfig): Evaluation = {
    val docLogLikelihood = gc.vector[Double](config.iterations)
    val wordLogLikelihood = gc.vector[Double](config.iterations)
    val tokenCounts = gc.vector[Long](config.iterations)
    val partitionCounts = gc.vector[Int](config.iterations)
    new Evaluation(config, docLogLikelihood, wordLogLikelihood, tokenCounts, partitionCounts)
  }
}