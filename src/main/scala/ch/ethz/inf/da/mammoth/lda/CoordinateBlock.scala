package ch.ethz.inf.da.mammoth.lda

import akka.util.Timeout
import breeze.linalg.Vector
import ch.ethz.inf.da.mammoth.util.timeSince
import com.typesafe.scalalogging.slf4j.Logger
import glint.models.client.granular.GranularBigMatrix

import scala.concurrent.{ExecutionContext, Future}

/**
  * A block of coordinates pulled from the parameter server using fast modulo arithmetic to check feature inclusion and
  * convert global features to block-local features
  *
  * @param block The block id
  * @param blocks The total number of blocks
  * @param model This block's partial model
  */
class CoordinateBlock(val block: Int, val blocks: Int, model: LDAModel, val wordTopicCounts: Array[Vector[Long]]) {

  @inline
  def contains(feature: Int): Boolean = (feature % blocks) == block

  @inline
  def mapping(feature: Int): Int = (feature - block) % blocks

  @inline
  def row(feature: Int): Vector[Long] = wordTopicCounts(mapping(feature))

}

/**
  * Companion object to pull coordinate blocks from the parameter servers
  */
object CoordinateBlock {

  /**
    * Pulls a block of coordinates from the parameter server
    *
    * @param block The block
    * @param blocks The total number of blocks
    * @param model The LDA model with references to the parameter server
    * @return A future containing the coordinate block
    */
  def apply(block: Int,
            blocks: Int,
            model: LDAModel)(implicit ec: ExecutionContext, timeout: Timeout): Future[CoordinateBlock] = {
    val granularModel = new GranularBigMatrix[Long](model.wordTopicCounts, model.config.topics, 10000)
    val pull = granularModel.pull(coordinates(block, blocks, model.config.vocabularyTerms))
    pull.map(wordTopicCounts => new CoordinateBlock(block, blocks, model, wordTopicCounts))
  }

  /**
    * Pulls a block of coordinates from the parameter server
    *
    * @param block The block
    * @param blocks The total number of blocks
    * @param model The LDA model with references to the parameter server
    * @param logger The logger to write timing information to
    * @return A future containing the coordinate block
    */
  def apply(block: Int,
            blocks: Int,
            model: LDAModel,
            logger: Logger)(implicit ec: ExecutionContext, timeout: Timeout): Future[CoordinateBlock] = {
    val start = System.currentTimeMillis()
    val pull = apply(block, blocks, model)
    pull.onComplete(_ => timeSince(start, logger, s"Coordinate block $block pull: "))
    pull
  }

  /**
    * Constructs an array of coordinates for given block
    *
    * @param block The block
    * @param blocks The total number of blocks
    * @param features The total number of features
    * @return An array of coordinates for given block
    */
  private def coordinates(block: Int, blocks: Int, features: Int): Array[Long] = {

    // Compute this coordinate blocks size
    var featureSize = 0
    var i = 0
    while (i < features) {
      if (i % blocks == block) {
        featureSize += 1
      }
      i += 1
    }

    // Construct and return feature array
    val result = new Array[Long](featureSize)
    var count = 0
    i = 0
    while (i < features) {
      if (i % blocks == block) {
        result(count) = i
        count += 1
      }
      i += 1
    }
    result

  }
}