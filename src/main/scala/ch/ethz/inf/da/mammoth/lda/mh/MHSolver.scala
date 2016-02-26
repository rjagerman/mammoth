package ch.ethz.inf.da.mammoth.lda.mh

import java.util.concurrent.Semaphore

import ch.ethz.inf.da.mammoth.lda._
import ch.ethz.inf.da.mammoth.util._
import glint.models.client.buffered.BufferedBigMatrix
import glint.models.client.granular.GranularBigMatrix

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * A block-coordinate metropolis-hastings based solver
  *
  * @param model The LDA model
  * @param id The identifier
  */
class MHSolver(model: LDAModel, sspClock: SSPClock, id: Int) extends Solver(model, sspClock, id) {

  val random = new FastRNG(model.config.seed + id)
  val granularWordTopicCounts = new GranularBigMatrix[Long](model.wordTopicCounts, model.config.topics, 10000)
  val mhSteps = 2
  val flushLock = new SimpleLock(32)

  /**
    * Runs the LDA inference algorithm on given partition of the data
    *
    * @param samples The samples to run the algorithm on
    * @param evaluation The evaluation reference to add measurements to
    * @param iteration The iteration number
    */
  override protected def fit(samples: Array[GibbsSample],
                             evaluation: Evaluation,
                             iteration: Int): Unit = {

    // Wait for previous iterations to finish globally (stale-synchronous protocol with bound τ)
    //sspClock.block(iteration - model.config.τ)

    // Get global counts
    val global = Await.result(model.topicCounts.pull((0L until model.config.topics).toArray), timeout.duration)

    // Constructed shuffled blocks (in a random order) and a function to pull a block of coordinates
    val blocks = Math.max(1, model.config.vocabularyTerms / model.config.blockSize)
    val shuffledBlocks = shuffledRange(random, blocks)
    def pullNext(block: Int): Future[CoordinateBlock] = {
      CoordinateBlock(shuffledBlocks(block), blocks, model, logger)
    }

    // Perform a pipelined iteration over the coordinate blocks which prefetches next coordinate blocks when possible
    val pipeline = new PipelinedFutureIterator(pullNext, blocks)
    pipeline.foreach { case futureCoordinateBlock =>

      // Log failures
      futureCoordinateBlock.onFailure { case ex => logger.error(ex.getMessage + "\n" + ex.getStackTraceString) }

      // Due to pipelining, the following Await call should be very quick for the majority of pulls since it's pulled
      // ahead of time during the previous iteration
      val coordinateBlock = time(logger, "Wait time: ") {
        Await.result(futureCoordinateBlock, (timeout.duration.toMillis * 4) milliseconds)
      }

      // Compute word-likelihood
      /*time(logger, "Word loglikelihood time: ") {
        evaluation.addLocalWordLikelihood(iteration, coordinateBlock.wordTopicCounts)
      }*/

      // Compute alias tables
      val aliasTables = time(logger, "Alias time: ") {
        computeAliasTables(coordinateBlock)
      }

      // Performing sampling on the coordinate block
      time(logger, "Sampling time: ") {
        sample(samples, coordinateBlock, aliasTables, global)
      }
    }

    // Compute doc-likelihood
    evaluation.addLocalDocLikelihood(iteration, samples)

    // Finish and count this iteration as done
    evaluation.finish(iteration)
    //sspClock.tick(id)

  }

  /**
    * Computes alias tables for given block of coordinates
    *
    * @param coordinateBlock The block of coordinates
    * @return An array with corresponding alias tables
    */
  private def computeAliasTables(coordinateBlock: CoordinateBlock): Array[AliasTable] = {
    val aliasTables = new Array[AliasTable](coordinateBlock.wordTopicCounts.length)
    var i = 0
    var sparsity: Double = 0.0
    while (i < coordinateBlock.wordTopicCounts.length) {
      var c = 0
      var d = 0
      while (c < coordinateBlock.wordTopicCounts(i).length) {
        if (coordinateBlock.wordTopicCounts(i)(c) == 0) {
          d += 1
        }
        c += 1
      }
      sparsity += d.toDouble / c.toDouble
      aliasTables(i) = new AliasTable(coordinateBlock.wordTopicCounts(i).map(x => x.toDouble + model.config.β))
      i += 1
    }
    logger.info(s"Avg alias sparsity: ${sparsity / coordinateBlock.wordTopicCounts.length.toDouble}")
    aliasTables
  }

  /**
    * Runs the LDA inference algorithm on given partition of the data and given block of coordinates
    *
    * @param samples The samples
    * @param coordinateBlock The block of coordinates
    */
  private def sample(samples: Array[GibbsSample],
                     coordinateBlock: CoordinateBlock,
                     aliasTables: Array[AliasTable],
                     global: Array[Long]): Unit = {

    // Construct sampler and buffers
    val sampler = new Sampler(model.config, mhSteps, random)
    sampler.globalCounts = global
    val globalDifference = new Array[Long](global.length)
    val bufferSize = 100000
    val buffer = new BufferedBigMatrix[Long](granularWordTopicCounts, bufferSize)
    flushLock.waitTime = 0L

    // Iterate over samples
    var i = 0
    while (i < samples.length) {
      val sample = samples(i)
      sampler.documentSize = sample.features.length
      sampler.documentTopicAssignments = sample.topics
      sampler.documentCounts = sample.denseCounts(model.config.topics)

      // Iterate over sample's features
      var j = 0
      while (j < sample.features.length) {
        val feature = sample.features(j)
        val oldTopic = sample.topics(j)
        if (coordinateBlock.contains(feature)) {
          sampler.wordCounts = coordinateBlock.row(feature)
          sampler.aliasTable = aliasTables(coordinateBlock.mapping(feature))

          // Perform a resampling step
          val newTopic = sampler.sampleFeature(feature, oldTopic)

          // Topic has changed, update all necessary counts
          if (newTopic != oldTopic) {
            sample.topics(j) = newTopic
            sampler.documentCounts(oldTopic) -= 1
            sampler.documentCounts(newTopic) += 1
            sampler.wordCounts(oldTopic) -= 1
            sampler.wordCounts(newTopic) += 1
            global(oldTopic) -= 1
            global(newTopic) += 1
            globalDifference(oldTopic) -= 1
            globalDifference(newTopic) += 1
            if (buffer.size >= bufferSize - 4) {
              flushLock.acquire()
              val flush = buffer.flush()
              flush.onComplete(_ => flushLock.release())
              flush.onFailure { case ex => logger.error(ex.getMessage + "\n" + ex.getStackTraceString) }
            }
            buffer.pushToBuffer(feature, oldTopic, -1)
            buffer.pushToBuffer(feature, newTopic, 1)

          }
        }

        j += 1
      }

      i += 1
    }

    // Flush final changes to parameter server
    flushLock.acquire()
    val flush = buffer.flush()
    flush.onComplete(_ => flushLock.release())
    flush.onFailure { case ex => logger.error(ex.getMessage + "\n" + ex.getStackTraceString) }
    model.topicCounts.push((0L until model.config.topics).toArray, globalDifference)

    // Log time spend waiting for the flush lock
    logger.info(s"Flush lock time: ${flushLock.waitTime}ms")

  }

}
